//! RDMA Transport implementation.
//!
//! This module provides a high-performance RDMA-based transport layer with:
//! - Consolidated state management via `TransportInner`
//! - Smart signal batching to prevent credit deadlocks
//! - Clean shutdown mechanism for background pollers
//!
//! Note: Updated to thread-per-core model (Rc/RefCell, !Send).

mod batch;
mod completion;
pub mod config;
mod flow_control;
pub mod protocol;
mod rdma_ops;
mod recv;
mod recv_pool;
mod send;
mod shutdown;
mod types;

pub use batch::{BatchResult, RdmaReadOp, RdmaWriteOp};
pub use config::TransportConfig;
pub(crate) use recv::alloc_recv_buffer;
pub(crate) use types::PostedRecvBuf;

use crate::drivers::rdma::buffer::RdmaBufferPool;
use crate::drivers::rdma::context::RdmaContext;
use crate::drivers::rdma::poller::{Completion, RdmaPoller, CompletionEvent};
use crate::transport::{BufferPool, Transport};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use flow_control::FlowController;
use protocol::{DEFAULT_MAX_RECV_BYTES, DEFAULT_RECV_DEPTH, MSG_HEADER_SIZE};
use recv_pool::RecvBufferManager;
use sideway::ibverbs::completion::{CompletionChannel, GenericCompletionQueue};
use sideway::ibverbs::queue_pair::{
    GenericQueuePair, PostSendGuard as _, QueuePair, QueuePairType, SetScatterGatherEntry,
    WorkRequestFlags,
};
use sideway::rdmacm::communication_manager::Identifier;
use std::collections::VecDeque;
use rustc_hash::FxHashMap;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::{Cell, RefCell};
use tracing::{debug, warn};
use flume::Receiver;
use std::os::fd::AsRawFd;

use completion::make_completion_future;
use recv::handle_recv_completion;
use shutdown::{graceful_shutdown, ShutdownConfig, ShutdownResult};
use types::PendingSend;

/// Consolidated mutable state, shared between transport and poller.
/// This reduces Rc overhead and simplifies state management.
pub(crate) struct TransportInner {
    pub(crate) qp: RefCell<GenericQueuePair>,
    pub(crate) pending_sends: RefCell<VecDeque<PendingSend>>,
    pub(crate) next_wr_id: Cell<u64>,
    pub(crate) send_not_signaled: Cell<u64>,
    pub(crate) recv_not_acked: Cell<u64>,
    pub(crate) shutdown_initiated: Cell<bool>,
    pub(crate) shutdown_received: Cell<bool>,
}

/// High-performance RDMA transport with credit-based flow control and signal batching.
#[derive(Clone)]
pub struct RdmaTransport {
    pub(crate) context: Arc<RdmaContext>,
    pub(crate) pool: Arc<RdmaBufferPool>,
    pub(crate) poller: Rc<RdmaPoller>,
    
    // Consolidated mutable state
    pub(crate) inner: Rc<TransportInner>,

    max_recv_bytes: usize,
    #[allow(dead_code)]
    cq: GenericCompletionQueue,
    #[allow(dead_code)]
    cm_id: Option<Arc<Identifier>>,

    recv_manager: Rc<RecvBufferManager>,
    recv_wc_rx: Rc<RefCell<Option<Receiver<CompletionEvent>>>>,
    posted_recvs: Rc<RefCell<FxHashMap<u64, PostedRecvBuf>>>,

    pub(crate) config: TransportConfig,
    flow_controller: FlowController,
    pub(crate) rdma_semaphore: FlowController,
    
    // Shutdown signaling: holding this Sender keeps the poller alive.
    // When all RdmaTransport clones are dropped, this Sender is dropped,
    // causing the poller's Recv to fail and the task to exit.
    _shutdown_tx: flume::Sender<()>,
}

pub(crate) struct RdmaTransportResources {
    pub context: Arc<RdmaContext>,
    pub cq: GenericCompletionQueue,
    pub channel: Arc<CompletionChannel>,
    pub qp: GenericQueuePair,
    pub config: TransportConfig,
    pub cm_id: Option<Arc<Identifier>>,
    pub initial_recvs: FxHashMap<u64, PostedRecvBuf>,
}

impl RdmaTransportResources {
    pub(crate) async fn into_transport(self) -> RdmaTransport {
        let (recv_wc_tx, recv_wc_rx) = flume::unbounded();
        let (shutdown_tx, shutdown_rx) = flume::bounded(1);
        
        let recv_manager = Rc::new(RecvBufferManager::new(DEFAULT_RECV_DEPTH));
        let flow_controller = FlowController::new(self.config.max_outstanding_sends);
        let rdma_semaphore = FlowController::new(self.config.max_rdma_wr);

        let poller = Rc::new(RdmaPoller::new(self.cq.clone(), self.channel.clone()));
        let (ready_tx, ready_rx) = flume::bounded::<()>(1);

        // Initialize consolidated inner state
        let max_wr = self.initial_recvs.keys().max().copied().unwrap_or(0);
        let inner = Rc::new(TransportInner {
            qp: RefCell::new(self.qp),
            pending_sends: RefCell::new(VecDeque::new()),
            next_wr_id: Cell::new(max_wr + 1),
            send_not_signaled: Cell::new(0),
            recv_not_acked: Cell::new(0),
            shutdown_initiated: Cell::new(false),
            shutdown_received: Cell::new(false),
        });

        let poller_clone = Rc::clone(&poller);
        let recv_tx_clone = recv_wc_tx.clone();
        let channel = Arc::clone(&self.channel);
        let flow_controller_clone = flow_controller.clone();
        let inner_clone = Rc::clone(&inner);
        
        monoio::spawn(async move {
            use std::os::unix::io::FromRawFd;
            let channel_fd = channel.as_raw_fd();
            // Dup FD to avoid closing the original when wrapping in UnixStream
            let dup_fd = unsafe { libc::dup(channel_fd) };
            if dup_fd < 0 {
                let _ = ready_tx.send(()); 
                return;
            }
            
            let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(dup_fd) };
            if std_stream.set_nonblocking(true).is_err() {
                let _ = ready_tx.send(());
                return;
            }
            
            let fd = match monoio::net::UnixStream::from_std(std_stream) {
                Ok(f) => f,
                Err(_) => {
                    let _ = ready_tx.send(());
                    return;
                }
            };

            if poller_clone.arm().is_err() {
                let _ = ready_tx.send(());
                return;
            }
            let _ = ready_tx.send(());

            // Hoist vector allocations outside the loop to avoid repeated allocations
            let mut recv_events = Vec::new();
            let mut send_events = Vec::new();

            loop {
                // Wait for FD readability OR shutdown signal
                // This prevents the "Zombie Task" issue
                monoio::select! {
                    res = fd.readable(false) => {
                        if res.is_err() { break; }
                    }
                    _ = shutdown_rx.recv_async() => {
                        debug!("RdmaPoller: shutdown signal received (channel closed)");
                        break;
                    }
                }

                loop {
                    match poller_clone.get_cq_event() {
                        Ok(true) => poller_clone.ack_events(1),
                        Ok(false) => break,
                        Err(_) => break,
                    }
                }

                if poller_clone.arm().is_err() { break; }

                // Clear vectors to recycle capacity
                recv_events.clear();
                send_events.clear();
                
                poller_clone.poll_cq(&mut recv_events, &mut send_events);
                
                for ev in recv_events.drain(..) {
                    if recv_tx_clone.send(ev).is_err() { return; }
                }
                
                if !send_events.is_empty() {
                    let max_wr_id = send_events.iter().map(|e| e.wr_id).max().unwrap_or(0);
                    let mut pending = inner_clone.pending_sends.borrow_mut();
                    let mut removed = 0;
                    while let Some(front) = pending.front() {
                        if front.wr_id <= max_wr_id {
                            pending.pop_front();
                            removed += 1;
                        } else {
                            break;
                        }
                    }
                    if removed > 0 {
                        flow_controller_clone.release(removed);
                    }
                }
            }
        });

        let _ = ready_rx.recv_async().await;

        let transport = RdmaTransport {
            pool: Arc::new(RdmaBufferPool::new(self.context.clone())),
            poller,
            inner,
            cq: self.cq,
            max_recv_bytes: DEFAULT_MAX_RECV_BYTES,
            cm_id: self.cm_id,
            recv_manager,
            recv_wc_rx: Rc::new(RefCell::new(Some(recv_wc_rx))),
            posted_recvs: Rc::new(RefCell::new(self.initial_recvs)),
            flow_controller,
            rdma_semaphore,
            config: self.config,
            context: self.context,
            _shutdown_tx: shutdown_tx,
        };

        let t_clone = transport.clone();
        monoio::spawn(async move {
            if let Err(e) = t_clone.init_receive_path().await {
                warn!("failed to init receive path: {}", e);
            }
        });

        transport
    }
}

impl RdmaTransport {
    #[inline]
    pub async fn new(context: Arc<RdmaContext>) -> io::Result<Self> {
        Self::with_config(context, TransportConfig::default()).await
    }

    pub async fn with_config(context: Arc<RdmaContext>, config: TransportConfig) -> io::Result<Self> {
        let channel = CompletionChannel::new(&context.ctx)
            .map_err(|e| io::Error::other(e.to_string()))?;
        channel.set_nonblocking(true)?;
        
        let cq: GenericCompletionQueue = context.ctx.create_cq_builder()
            .setup_comp_channel(&channel, 0)
            .build()
            .map_err(|e| io::Error::other(e.to_string()))?
            .into();

        let qp = Self::build_queue_pair(&context, &cq, &config)?;

        Ok(RdmaTransportResources {
            context, cq, channel, qp, config, cm_id: None, initial_recvs: FxHashMap::default(),
        }.into_transport().await)
    }

    pub(crate) fn build_queue_pair(
        context: &RdmaContext,
        cq: &GenericCompletionQueue,
        config: &TransportConfig,
    ) -> io::Result<GenericQueuePair> {
        let desired_send_wr = DEFAULT_RECV_DEPTH.max(
            config.max_outstanding_sends.saturating_add(config.max_rdma_wr).saturating_add(64),
        );

        let mut builder = context.pd.create_qp_builder();
        builder.setup_send_cq(cq.clone());
        builder.setup_recv_cq(cq.clone());
        builder.setup_qp_type(QueuePairType::ReliableConnection);
        builder.setup_max_send_wr((desired_send_wr.min(u32::MAX as usize)) as u32)
            .setup_max_send_sge(1)
            .setup_max_recv_wr(DEFAULT_RECV_DEPTH as u32)
            .setup_max_recv_sge(1);

        let qp = builder.build().map_err(|e| io::Error::other(e.to_string()))?;
        Ok(GenericQueuePair::Basic(qp))
    }

    pub(crate) fn alloc_wr_id(&self) -> u64 {
        let id = self.inner.next_wr_id.get();
        self.inner.next_wr_id.set(id.wrapping_add(1));
        id
    }

    fn recv_buf_len(&self) -> usize {
        MSG_HEADER_SIZE + self.max_recv_bytes
    }

    async fn init_receive_path(&self) -> io::Result<()> {
        let wc_rx = {
            let mut guard = self.recv_wc_rx.borrow_mut();
            guard.take().ok_or_else(|| io::Error::other("receive path already initialized"))?
        };

        let recv_len = self.recv_buf_len();
        let depth = self.recv_manager.target_depth;
        
        let current_posted = self.posted_recvs.borrow().len();
        let depth_needed = depth.saturating_sub(current_posted);

        let posted_cnt = if depth_needed > 0 {
            recv::pre_post_receive_buffers(
                &self.inner.qp, // Updated to use inner
                &self.posted_recvs, 
                &self.context, 
                &self.pool, 
                &self.inner.next_wr_id, 
                recv_len, 
                depth_needed
            ).await
        } else {
            0
        };
        
        let total_posted = current_posted + posted_cnt;

        if total_posted == 0 {
            *self.recv_wc_rx.borrow_mut() = Some(wc_rx);
            return Err(io::Error::other("failed to post any initial receive buffers"));
        }

        // Context updated to hold inner state
        let recv_ctx = recv::RecvContext {
            inner: Rc::clone(&self.inner),
            posted_recvs: Rc::clone(&self.posted_recvs),
            recv_manager: Rc::clone(&self.recv_manager),
            context: Arc::clone(&self.context),
            pool: Arc::clone(&self.pool),
            flow_controller: self.flow_controller.clone(),
            recv_len,
            buf_ack_batch: self.config.buf_ack_batch,
        };

        monoio::spawn(async move {
            while let Ok(ev) = wc_rx.recv_async().await {
                handle_recv_completion(ev, &recv_ctx).await;
            }
        });

        Ok(())
    }

    pub fn wait_for_completion(&self, wr_id: u64) -> impl Future<Output = io::Result<Completion>> + '_ {
        make_completion_future(Rc::clone(&self.poller), wr_id)
    }

    pub async fn shutdown(&self) -> io::Result<()> {
        let config = ShutdownConfig::default();
        let result = graceful_shutdown(
            &self.inner,
            &self.poller,
            &config,
        ).await;

        match result {
            ShutdownResult::Graceful => debug!("graceful shutdown completed"),
            ShutdownResult::ForcedError => debug!("shutdown forced QP to Error state"),
            ShutdownResult::AlreadyShuttingDown => debug!("shutdown already in progress"),
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl Transport for RdmaTransport {
    async fn send(&self, buf: Bytes) -> io::Result<()> {
        // 1. Acquire send credit
        let _ = self.flow_controller.acquire().await;

        let wr_id = self.alloc_wr_id();
        let total_len = MSG_HEADER_SIZE + buf.len();
        let (lkey, addr, len, keepalive) = send::prepare_send_buffer(&self.context, &buf, total_len)?;

        // 2. Smart Signaling: prevent Deadlock by forcing signal if credits are low
        let sends_since_signal = self.inner.send_not_signaled.get();
        self.inner.send_not_signaled.set(sends_since_signal + 1);
        
        // Note: FlowController needs `available()` method added
        let credits_low = self.flow_controller.available() < self.config.send_signal_batch;
        let batch_reached = sends_since_signal + 1 >= self.config.send_signal_batch as u64;
        let should_signal = batch_reached || credits_low;

        let flags = if should_signal {
            self.inner.send_not_signaled.set(0);
            WorkRequestFlags::Signaled
        } else {
            WorkRequestFlags::none()
        };

        {
            let mut pending = self.inner.pending_sends.borrow_mut();
            pending.push_back(PendingSend {
                wr_id,
                _keepalive: keepalive,
            });
        }

        {
            let mut qp = self.inner.qp.borrow_mut();
            let mut guard = qp.start_post_send();
            let wr = guard.construct_wr(wr_id, flags);
            unsafe {
                wr.setup_send().setup_sge(lkey, addr, len);
            }
            if let Err(e) = guard.post() {
                let mut pending = self.inner.pending_sends.borrow_mut();
                if let Some(pos) = pending.iter().position(|p| p.wr_id == wr_id) {
                    pending.remove(pos);
                }
                self.flow_controller.release(1);
                return Err(io::Error::other(e.to_string()));
            }
        }

        Ok(())
    }

    async fn recv(&self) -> io::Result<Bytes> {
        self.recv_manager
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "receive channel closed"))
    }

    #[inline]
    fn alloc_buf(&self, size: usize) -> BytesMut {
        self.pool.alloc(size)
    }
}