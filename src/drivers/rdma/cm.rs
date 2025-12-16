use crate::drivers::rdma::context::RdmaContext;
use crate::drivers::rdma::transport::{RdmaTransport, TransportConfig};
use sideway::ibverbs::completion::GenericCompletionQueue;
use sideway::ibverbs::queue_pair::{GenericQueuePair, QueuePair, QueuePairState};
use sideway::rdmacm::communication_manager::{
    ConnectionParameter, EventChannel, EventType, Identifier, PortSpace,
};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct RdmaListener {
    event_channel: Arc<EventChannel>,
    _listener_id: Arc<Identifier>,
    accept_lock: Mutex<()>,
    timeout: Duration,
    max_send_wr: u32,
    max_recv_wr: u32,
}

impl RdmaListener {
    pub fn local_addr(&self) -> SocketAddr {
        // TODO: sideway v0.4.0 doesn't expose rdma_get_local_addr wrapper.
        // Returning placeholder until upstream support is added.
        // See: https://man7.org/linux/man-pages/man3/rdma_get_local_addr.3.html
        SocketAddr::from(([0, 0, 0, 0], 0))
    }

    pub async fn accept(&self) -> io::Result<RdmaTransport> {
        let _g = self.accept_lock.lock().await;

        let event_channel = self.event_channel.clone();
        let timeout = self.timeout;
        let max_send_wr = self.max_send_wr;
        let max_recv_wr = self.max_recv_wr;

        tokio::task::spawn_blocking(move || blocking_accept(event_channel, timeout, max_send_wr, max_recv_wr))
            .await
            .map_err(|e| io::Error::other(format!("join error: {e}")))?
    }
}

pub async fn listen(bind_addr: SocketAddr, backlog: i32) -> io::Result<RdmaListener> {
    let event_channel = EventChannel::new()
        .map_err(|e| io::Error::other(e.to_string()))?;
    let id = event_channel
        .create_id(PortSpace::Tcp)
        .map_err(|e| io::Error::other(e.to_string()))?;

    id.bind_addr(bind_addr)
        .map_err(|e| io::Error::other(e.to_string()))?;
    id.listen(backlog)
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(RdmaListener {
        event_channel,
        _listener_id: id,
        accept_lock: Mutex::new(()),
        timeout: Duration::from_secs(1),
        max_send_wr: 512,
        max_recv_wr: 512,
    })
}

pub async fn connect(dst_addr: SocketAddr, timeout: Duration) -> io::Result<RdmaTransport> {
    connect_with_config(dst_addr, timeout, TransportConfig::default()).await
}

pub async fn connect_with_config(
    dst_addr: SocketAddr,
    timeout: Duration,
    config: TransportConfig,
) -> io::Result<RdmaTransport> {
    let event_channel = EventChannel::new().map_err(|e| io::Error::other(e.to_string()))?;
    let id = event_channel
        .create_id(PortSpace::Tcp)
        .map_err(|e| io::Error::other(e.to_string()))?;

    let ec = event_channel.clone();
    tokio::task::spawn_blocking(move || blocking_connect(ec, id, dst_addr, timeout, config))
        .await
        .map_err(|e| io::Error::other(format!("join error: {e}")))?
}

fn build_connected_transport(
    cm_id: Arc<Identifier>,
    rdma_ctx: Arc<RdmaContext>,
    cq: GenericCompletionQueue,
    qp: GenericQueuePair,
    config: TransportConfig,
) -> RdmaTransport {
    RdmaTransport::from_connected_with_config(cm_id, rdma_ctx, cq, qp, config)
}

fn blocking_connect(
    event_channel: Arc<EventChannel>,
    id: Arc<Identifier>,
    dst_addr: SocketAddr,
    timeout: Duration,
    config: TransportConfig,
) -> io::Result<RdmaTransport> {
    id.resolve_addr(None, dst_addr, timeout)
        .map_err(|e| io::Error::other(e.to_string()))?;

    let mut resources: Option<(
        Arc<RdmaContext>,
        GenericCompletionQueue,
        GenericQueuePair,
        TransportConfig,
    )> = None;

    loop {
        let event = event_channel
            .get_cm_event()
            .map_err(|e| io::Error::other(e.to_string()))?;

        match event.event_type() {
            EventType::AddressResolved => {
                id.resolve_route(timeout)
                    .map_err(|e| io::Error::other(e.to_string()))?;
            }
            EventType::RouteResolved => {
                let dev_ctx = id
                    .get_device_context()
                    .ok_or_else(|| io::Error::other("no device context after route resolved"))?;
                let rdma_ctx = RdmaContext::from_device_context(dev_ctx)?;

                let cq: GenericCompletionQueue = rdma_ctx
                    .ctx
                    .create_cq_builder()
                    .setup_cqe(1024)
                    .build()
                    .map_err(|e| io::Error::other(e.to_string()))?
                    .into();

                // Use the standardized QP builder from transport configuration
                let mut qp = RdmaTransport::build_queue_pair(&rdma_ctx, &cq, &config)?;
                
                let attr = id
                    .get_qp_attr(QueuePairState::Init)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                qp.modify(&attr)
                    .map_err(|e| io::Error::other(e.to_string()))?;

                let mut param = ConnectionParameter::new();
                param.setup_qp_number(qp.qp_number());
                id.connect(param)
                    .map_err(|e| io::Error::other(e.to_string()))?;

                resources = Some((rdma_ctx, cq, qp, config.clone()));
            }
            EventType::ConnectResponse => {
                let (rdma_ctx, _cq, ref mut qp, _) = resources
                    .as_mut()
                    .ok_or_else(|| io::Error::other("connect response before resources"))?;

                let attr = id
                    .get_qp_attr(QueuePairState::ReadyToReceive)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                qp.modify(&attr)
                    .map_err(|e| io::Error::other(e.to_string()))?;

                let attr = id
                    .get_qp_attr(QueuePairState::ReadyToSend)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                qp.modify(&attr)
                    .map_err(|e| io::Error::other(e.to_string()))?;

                id.establish()
                    .map_err(|e| io::Error::other(e.to_string()))?;

                // keep rdma_ctx alive; actual return happens on Established
                let _ = rdma_ctx;
            }
            EventType::Established => {
                let (rdma_ctx, cq, qp, config) = resources
                    .take()
                    .ok_or_else(|| io::Error::other("established before resources"))?;
                return Ok(build_connected_transport(
                    id.clone(),
                    rdma_ctx,
                    cq,
                    qp,
                    config,
                ));
            }
            EventType::ConnectError | EventType::Rejected | EventType::Unreachable => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("rdma cm connect failed: {:?}", event.event_type()),
                ));
            }
            EventType::AddressError | EventType::RouteError => {
                return Err(io::Error::other(format!(
                    "rdma cm resolve failed: {:?}",
                    event.event_type()
                )));
            }
            _ => {}
        }
    }
}

fn blocking_accept(
    event_channel: Arc<EventChannel>,
    timeout: Duration,
    _max_send_wr: u32,
    _max_recv_wr: u32,
) -> io::Result<RdmaTransport> {
    use std::collections::HashMap;

    // Track multiple pending connections by their Arc pointer address
    let mut pending: HashMap<
        usize,
        (
            Arc<Identifier>,
            Arc<RdmaContext>,
            GenericCompletionQueue,
            GenericQueuePair,
        ),
    > = HashMap::new();

    loop {
        let event = event_channel
            .get_cm_event()
            .map_err(|e| io::Error::other(e.to_string()))?;

        match event.event_type() {
            EventType::ConnectRequest => {
                let new_id = event
                    .cm_id()
                    .ok_or_else(|| io::Error::other("connect request without cm_id"))?;

                let dev_ctx = new_id
                    .get_device_context()
                    .ok_or_else(|| io::Error::other("no device context for connect request"))?;
                // TODO: For production, consider sharing RdmaContext (and its PD) across
                // connections to the same device for better memory region sharing.
                let rdma_ctx = RdmaContext::from_device_context(dev_ctx)?;

                let cq: GenericCompletionQueue = rdma_ctx
                    .ctx
                    .create_cq_builder()
                    .setup_cqe(1024)
                    .build()
                    .map_err(|e| io::Error::other(e.to_string()))?
                    .into();

                // Use default config for accepted connections for now
                let config = TransportConfig::default();
                let mut qp = RdmaTransport::build_queue_pair(&rdma_ctx, &cq, &config)?;

                for state in [
                    QueuePairState::Init,
                    QueuePairState::ReadyToReceive,
                    QueuePairState::ReadyToSend,
                ] {
                    let attr = new_id
                        .get_qp_attr(state)
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    qp.modify(&attr)
                        .map_err(|e| io::Error::other(e.to_string()))?;
                }

                let mut param = ConnectionParameter::new();
                param.setup_qp_number(qp.qp_number());
                new_id
                    .accept(param)
                    .map_err(|e| io::Error::other(e.to_string()))?;

                let id_ptr = Arc::as_ptr(&new_id) as usize;
                pending.insert(id_ptr, (new_id, rdma_ctx, cq, qp));
            }
            EventType::Established => {
                let cm_id = event
                    .cm_id()
                    .ok_or_else(|| io::Error::other("established event without cm_id"))?;

                let id_ptr = Arc::as_ptr(&cm_id) as usize;

                if let Some((cm_id, rdma_ctx, cq, qp)) = pending.remove(&id_ptr) {
                    return Ok(build_connected_transport(
                        cm_id,
                        rdma_ctx,
                        cq,
                        qp,
                        TransportConfig::default(),
                    ));
                }
                // Ignore Established for unknown connections (shouldn't happen)
            }
            EventType::Disconnected | EventType::Rejected => {
                // Clean up any matching pending connection
                if let Some(cm_id) = event.cm_id() {
                    let id_ptr = Arc::as_ptr(&cm_id) as usize;
                    pending.remove(&id_ptr);
                }
            }
            _ => {
                // ignore other events
                let _ = timeout;
            }
        }
    }
}


