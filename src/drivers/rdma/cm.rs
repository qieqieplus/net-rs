//! RDMA Connection Manager
//!
//! Provides async-friendly wrappers around RDMA CM for establishing connections.

use crate::drivers::rdma::context::RdmaContext;
use crate::drivers::rdma::transport::{RdmaTransport, RdmaTransportResources, TransportConfig};
use flume::Receiver;
use sideway::ibverbs::completion::{CompletionChannel, GenericCompletionQueue};
use sideway::ibverbs::queue_pair::{GenericQueuePair, QueuePair, QueuePairState, SetScatterGatherEntry};
use sideway::rdmacm::communication_manager::{
    ConnectionParameter, Event, EventChannel, EventType, Identifier, PortSpace,
};
use crate::drivers::rdma::transport::protocol::{DEFAULT_RECV_DEPTH, MSG_HEADER_SIZE, DEFAULT_MAX_RECV_BYTES};
use crate::drivers::rdma::transport::{alloc_recv_buffer, PostedRecvBuf};
use crate::drivers::rdma::buffer::RdmaBufferPool;
use rustc_hash::FxHashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, warn};

/// Helper trait to convert any Display error to io::Error.
trait IntoIoError<T> {
    fn io_err(self) -> io::Result<T>;
}

impl<T, E: std::fmt::Display> IntoIoError<T> for Result<T, E> {
    fn io_err(self) -> io::Result<T> {
        self.map_err(|e| io::Error::other(e.to_string()))
    }
}

fn pre_post_recvs_sync(
    ctx: &Arc<RdmaContext>,
    qp: &mut GenericQueuePair,
) -> io::Result<FxHashMap<u64, PostedRecvBuf>> {
    let pool = Arc::new(RdmaBufferPool::new(ctx.clone()));
    let recv_len = MSG_HEADER_SIZE + DEFAULT_MAX_RECV_BYTES;
    let mut map = FxHashMap::default();
    
    // Start from 1. 
    for i in 0..DEFAULT_RECV_DEPTH {
        let wr_id = (i + 1) as u64;
        let buf = alloc_recv_buffer(ctx, &pool, recv_len)?;
        let (lkey, addr, len) = buf.sge(recv_len);
        
        {
             let mut guard = qp.start_post_recv();
             unsafe {
                 guard.construct_wr(wr_id).setup_sge(lkey, addr, len);
             }
             guard.post().map_err(|e| io::Error::other(e.to_string()))?;
        }
        map.insert(wr_id, buf);
    }
    Ok(map)
}

/// Run a blocking closure on a thread and await result via channel.
/// 
/// With monoio's `sync` feature enabled, flume's waker can interrupt
/// io_uring_enter() via eventfd, so we can use recv_async() directly.
async fn spawn_blocking<F, R>(f: F) -> io::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = flume::bounded(1);
    
    std::thread::spawn(move || {
        let result = f();
        let _ = tx.send(result);
    });

    rx.recv_async()
        .await
        .map_err(|e| io::Error::other(format!("spawn_blocking failed: {}", e)))
}


type PendingConn = (
    Arc<Identifier>,
    Arc<RdmaContext>,
    GenericCompletionQueue,
    Arc<CompletionChannel>,
    GenericQueuePair,
    FxHashMap<u64, PostedRecvBuf>,
);

type ConnectResources = (
    Arc<RdmaContext>,
    GenericCompletionQueue,
    Arc<CompletionChannel>,
    GenericQueuePair,
    TransportConfig,
    FxHashMap<u64, PostedRecvBuf>,
);

/// RDMA connection listener using a background thread model for monoio compatibility.
pub struct RdmaListener {
    accept_rx: Receiver<io::Result<RdmaTransportResources>>,
    _listener_id: Arc<Identifier>,
}

impl RdmaListener {
    /// Get the local socket address (placeholder - sideway doesn't expose rdma_get_local_addr).
    pub fn local_addr(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], 0))
    }

    /// Accept a new RDMA connection.
    pub async fn accept(&self) -> io::Result<RdmaTransport> {
        match self.accept_rx.recv_async().await {
            Ok(result) => Ok(result?.into_transport().await),
            Err(_) => Err(io::Error::other("listener closed")),
        }
    }
}



/// Start listening for RDMA connections. Spawns a background thread for CM events.
/// Uses the first available RDMA device.
pub async fn listen(bind_addr: SocketAddr, backlog: i32) -> io::Result<RdmaListener> {
    // Get the first available device name
    let devices = crate::drivers::rdma::get_device_list();
    let dev_name = devices
        .first()
        .ok_or_else(|| io::Error::other("no RDMA devices available"))?
        .clone();
    listen_with_device(bind_addr, backlog, &dev_name).await
}

/// Start listening for RDMA connections on a specific device.
pub async fn listen_with_device(bind_addr: SocketAddr, backlog: i32, dev_name: &str) -> io::Result<RdmaListener> {
    // Create stable context by opening device by name (not from RDMACM)
    let ctx = RdmaContext::open(dev_name)?;
    
    let ec = EventChannel::new().io_err()?;
    let id = ec.create_id(PortSpace::Tcp).io_err()?;
    id.bind_addr(bind_addr).io_err()?;
    id.listen(backlog).io_err()?;

    let (tx, rx) = flume::unbounded();
    let ec2 = ec.clone();
    std::thread::Builder::new()
        .name(format!("rdma-cm-{}", bind_addr))
        .spawn(move || {
            run_listener_loop(ec2, ctx, tx);
        })
        .io_err()?;

    Ok(RdmaListener {
        accept_rx: rx,
        _listener_id: id,
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
    let ec = EventChannel::new().io_err()?;
    let id = ec.create_id(PortSpace::Tcp).io_err()?;

    let resources =
        spawn_blocking(move || blocking_connect(ec, id, dst_addr, timeout, config)).await??;

    Ok(resources.into_transport().await)
}

fn blocking_connect(
    ec: Arc<EventChannel>,
    id: Arc<Identifier>,
    dst_addr: SocketAddr,
    timeout: Duration,
    config: TransportConfig,
) -> io::Result<RdmaTransportResources> {
    eprintln!("[cm::blocking_connect] resolve_addr to {:?}...", dst_addr);
    id.resolve_addr(None, dst_addr, timeout).io_err()?;
    eprintln!("[cm::blocking_connect] resolve_addr done, waiting for CM events...");

    let mut res: Option<ConnectResources> = None;

    loop {
        let event = ec.get_cm_event().io_err()?;
        let event_type = event.event_type();
        eprintln!("[cm::blocking_connect] CM event: {:?}", event_type);

        match event_type {
            EventType::AddressResolved => {
                eprintln!("[cm::blocking_connect] resolve_route...");
                id.resolve_route(timeout).io_err()?;
                eprintln!("[cm::blocking_connect] resolve_route done, waiting for RouteResolved...");
            }
            EventType::RouteResolved => {
                let dev = id
                    .get_device_context()
                    .ok_or_else(|| io::Error::other("no device context"))?;
                let ctx = RdmaContext::from_device_context(dev)?;
                let channel = CompletionChannel::new(&ctx.ctx)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                channel.set_nonblocking(true)?;
                let cq: GenericCompletionQueue = ctx
                    .ctx
                    .create_cq_builder()
                    .setup_cqe(1024)
                    .setup_comp_channel(&channel, 0)
                    .build()
                    .io_err()?
                    .into();
                let mut qp = RdmaTransport::build_queue_pair(&ctx, &cq, &config)?;

                let attr = id.get_qp_attr(QueuePairState::Init).io_err()?;
                qp.modify(&attr).io_err()?;
                
                let initial_recvs = pre_post_recvs_sync(&ctx, &mut qp)?;

                let mut param = ConnectionParameter::new();
                param.setup_qp_number(qp.qp_number());
                id.connect(param).io_err()?;
                res = Some((ctx, cq, channel, qp, config.clone(), initial_recvs));
            }
            EventType::ConnectResponse => {
                eprintln!("[cm::blocking_connect] handling ConnectResponse...");
                let (ctx, cq, channel, qp, cfg, initial_recvs) = res.take().ok_or_else(|| io::Error::other("no resources"))?;

                // For active (client) side: after ConnectResponse, transition QP to RTR/RTS.
                let mut qp = qp;
                eprintln!("[cm::blocking_connect] transitioning QP to RTR...");
                qp.modify(&id.get_qp_attr(QueuePairState::ReadyToReceive).io_err()?)
                    .io_err()?;
                eprintln!("[cm::blocking_connect] transitioning QP to RTS...");
                qp.modify(&id.get_qp_attr(QueuePairState::ReadyToSend).io_err()?)
                    .io_err()?;

                // Send RTU (Ready To Use) and return immediately.
                eprintln!("[cm::blocking_connect] calling establish()...");
                id.establish().io_err()?;
                eprintln!("[cm::blocking_connect] establish() done, returning transport!");

                return Ok(RdmaTransportResources {
                    context: ctx,
                    cq,
                    channel,
                    qp,
                    config: cfg,
                    cm_id: Some(id.clone()),
                    initial_recvs,
                });
            }
            EventType::Established => {
                // Should already be handled by ConnectResponse on client,
                // but handle it here just in case of different RDMACM behavior.
                if let Some((ctx, cq, channel, qp, cfg, initial_recvs)) = res.take() {
                    return Ok(RdmaTransportResources {
                        context: ctx,
                        cq,
                        channel,
                        qp,
                        config: cfg,
                        cm_id: Some(id.clone()),
                        initial_recvs,
                    });
                }
            }
            EventType::ConnectError | EventType::Rejected | EventType::Unreachable => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("{:?}", event_type),
                ));
            }
            EventType::AddressError | EventType::RouteError => {
                return Err(io::Error::other(format!(
                    "resolve failed: {:?}",
                    event_type
                )));
            }
            _ => {
                debug!("[cm] ignoring event: {:?}", event_type);
            }
        }
    }
}

/// Background event loop for listener - runs forever, sends established connections via channel.
fn run_listener_loop(
    ec: Arc<EventChannel>,
    ctx: Arc<RdmaContext>,
    tx: flume::Sender<io::Result<RdmaTransportResources>>,
) {
    let mut pending: FxHashMap<usize, PendingConn> = FxHashMap::default();

    eprintln!("[cm::listener_loop] starting listener loop");
    loop {
        let event = match ec.get_cm_event() {
            Ok(e) => e,
            Err(e) => {
                error!("[cm::listener] get_cm_event failed: {}", e);
                let _ = tx.send(Err(io::Error::other(e.to_string())));
                return;
            }
        };

        let event_type = event.event_type();
        eprintln!("[cm::listener_loop] CM event: {:?}", event_type);

        match event_type {
            EventType::ConnectRequest => {
                eprintln!("[cm::listener_loop] got ConnectRequest!");
                if let Err(e) = handle_connect_request(&event, &ctx, &mut pending) {
                    warn!("[cm::listener] connect request failed: {e}");
                }
            }
            EventType::Established => {
                let Some(cm_id) = event.cm_id() else {
                    continue;
                };
                let key = Arc::as_ptr(&cm_id) as usize;
                if let Some((id, ctx, cq, channel, qp, initial_recvs)) = pending.remove(&key) {
                    let r = RdmaTransportResources {
                        context: ctx,
                        cq,
                        channel,
                        qp,
                        config: TransportConfig::default(),
                        cm_id: Some(id),
                        initial_recvs,
                    };
                    if tx.send(Ok(r)).is_err() {
                        return;
                    }
                    // flume's send will wake up the monoio reactor via eventfd (sync feature)

                } else {
                    eprintln!("[cm::listener_loop] warning: established event for unknown id key={}", key);
                }
            }
            EventType::Disconnected
            | EventType::Rejected
            | EventType::ConnectError
            | EventType::Unreachable => {
                if let Some(id) = event.cm_id() {
                    pending.remove(&(Arc::as_ptr(&id) as usize));
                }
            }
            _ => {}
        }
    }
}

fn handle_connect_request(
    event: &Event,
    ctx: &Arc<RdmaContext>,
    pending: &mut FxHashMap<usize, PendingConn>,
) -> io::Result<()> {
    let id = event.cm_id().ok_or_else(|| io::Error::other("no cm_id"))?;

    // Use the pre-created stable context (opened by device name)
    let ctx = Arc::clone(ctx);

    let channel = CompletionChannel::new(&ctx.ctx)
        .map_err(|e| {
            let os_err = io::Error::last_os_error();
            io::Error::other(format!("failed to create completion channel: {:?} (OS hint: {:?})", e, os_err))
        })?;
    channel.set_nonblocking(true)?;
    let cq: GenericCompletionQueue = ctx
        .ctx
        .create_cq_builder()
        .setup_cqe(1024)
        .setup_comp_channel(&channel, 0)
        .build()
        .io_err()?
        .into();

    let mut qp = RdmaTransport::build_queue_pair(&ctx, &cq, &TransportConfig::default())?;
    
    qp.modify(&id.get_qp_attr(QueuePairState::Init).io_err()?).io_err()?;

    let initial_recvs = pre_post_recvs_sync(&ctx, &mut qp)?;

    for state in [
        QueuePairState::ReadyToReceive,
        QueuePairState::ReadyToSend,
    ] {
        qp.modify(&id.get_qp_attr(state).io_err()?).io_err()?;
    }

    let mut param = ConnectionParameter::new();
    param.setup_qp_number(qp.qp_number());
    id.accept(param).io_err()?;

    let key = Arc::as_ptr(&id) as usize;
    pending.insert(key, (id, ctx, cq, channel, qp, initial_recvs));

    Ok(())
}
