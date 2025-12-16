//! RDMA Transport implementation.
//!
//! This module provides a high-performance RDMA-based transport layer with:
//! - Credit-based flow control for backpressure
//! - Signal batching for reduced completion overhead
//! - Slab allocation for efficient buffer management
//! - One-sided RDMA READ/WRITE operations for zero-copy transfers
//!
//! # Architecture
//!
//! The transport is built on top of the `sideway` RDMA library and integrates with
//! the generic `Transport` trait. Key components:
//!
//! - **FlowController**: Semaphore-based credit system preventing sender overflow
//! - **RecvBufferManager**: Async channel decoupling completions from recv() calls
//! - **RdmaPoller**: Background thread polling the completion queue
//! - **SlabAllocator**: Pre-registered memory pools avoiding per-send registration
//!
//! # Module Structure
//!
//! - `config` - Transport configuration parameters
//! - `protocol` - Wire protocol constants and encoding/decoding
//! - `types` - Internal type definitions
//! - `completion` - Completion future implementations
//! - `send` - Send path helpers
//! - `recv` - Receive path logic
//! - `rdma_ops` - RDMA READ/WRITE operations

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

use crate::drivers::rdma::buffer::{RdmaBufferPool, RdmaMr};
use crate::drivers::rdma::context::RdmaContext;
use crate::drivers::rdma::poller::{Completion, RdmaPoller};
use crate::transport::{BufferPool, Transport};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use flow_control::FlowController;
use protocol::{DEFAULT_MAX_RECV_BYTES, DEFAULT_RECV_DEPTH};
use recv_pool::RecvBufferManager;
use sideway::ibverbs::completion::GenericCompletionQueue;
use sideway::ibverbs::queue_pair::{
    GenericQueuePair, PostSendGuard as _, QueuePair, QueuePairType, SetScatterGatherEntry,
    WorkRequestFlags,
};
use sideway::rdmacm::communication_manager::Identifier;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{mpsc, Mutex, OnceCell};
use tracing::{debug, warn};

use completion::make_completion_future;
use recv::{alloc_recv_buffer, handle_recv_completion, post_recv_wr};
use send::{PostSendGuard, SignaledSendReaper};
use shutdown::{graceful_shutdown, ShutdownConfig, ShutdownResult};
use types::{PendingSend, PostedRecvBuf, SendKeepalive};

/// High-performance RDMA transport with credit-based flow control and signal batching.
pub struct RdmaTransport {
    pub(crate) context: Arc<RdmaContext>,
    pub(crate) pool: Arc<RdmaBufferPool>,
    pub(crate) poller: Arc<RdmaPoller>,
    pub(crate) qp: Arc<Mutex<GenericQueuePair>>,
    pub(crate) next_wr_id: Arc<AtomicU64>,
    max_recv_bytes: usize,
    #[allow(dead_code)]
    cq: GenericCompletionQueue,
    #[allow(dead_code)]
    cm_id: Option<Arc<Identifier>>,

    // Lazy initialization
    started: OnceCell<()>,
    recv_manager: Arc<RecvBufferManager>,
    recv_wc_rx: Mutex<Option<mpsc::UnboundedReceiver<crate::drivers::rdma::poller::CompletionEvent>>>,
    posted_recvs: Arc<DashMap<u64, PostedRecvBuf>>,

    // Flow control and batching state
    pub(crate) config: TransportConfig,
    flow_controller: Arc<FlowController>,
    recv_not_acked: Arc<AtomicU64>,
    send_not_signaled: Arc<AtomicU64>,

    // RDMA concurrency control
    pub(crate) rdma_semaphore: Arc<tokio::sync::Semaphore>,

    // Shutdown state
    shutdown_initiated: Arc<AtomicBool>,
    shutdown_received: Arc<AtomicBool>,

    // Pending unsignaled sends (must be kept alive until signaled completion)
    pending_sends: Arc<StdMutex<VecDeque<PendingSend>>>,
}

// SAFETY: RdmaTransport is Send+Sync because all its fields are either:
// - Arc<T> where T: Send+Sync (context, pool, poller, etc.)
// - Mutex-protected types (qp, recv_wc_rx)
// - Atomics (next_wr_id, shutdown flags)
// The underlying RDMA resources are thread-safe when accessed through the ibverbs API.
unsafe impl Send for RdmaTransport {}
unsafe impl Sync for RdmaTransport {}

/// Internal struct for building transport state, used to deduplicate constructor logic.
struct TransportParts {
    context: Arc<RdmaContext>,
    cq: GenericCompletionQueue,
    qp: GenericQueuePair,
    config: TransportConfig,
    cm_id: Option<Arc<Identifier>>,
}

impl TransportParts {
    /// Build the full RdmaTransport from pre-configured parts.
    fn into_transport(self) -> RdmaTransport {
        let (recv_wc_tx, recv_wc_rx) = mpsc::unbounded_channel();
        
        RdmaTransport {
            pool: Arc::new(RdmaBufferPool::new(self.context.clone())),
            poller: Arc::new(RdmaPoller::new_with_recv(self.cq.clone(), Some(recv_wc_tx))),
            qp: Arc::new(Mutex::new(self.qp)),
            cq: self.cq,
            next_wr_id: Arc::new(AtomicU64::new(1)),
            max_recv_bytes: DEFAULT_MAX_RECV_BYTES,
            cm_id: self.cm_id,

            started: OnceCell::new(),
            recv_manager: RecvBufferManager::new(DEFAULT_RECV_DEPTH),
            recv_wc_rx: Mutex::new(Some(recv_wc_rx)),
            posted_recvs: Arc::new(DashMap::new()),

            flow_controller: Arc::new(FlowController::new(self.config.max_outstanding_sends)),
            recv_not_acked: Arc::new(AtomicU64::new(0)),
            send_not_signaled: Arc::new(AtomicU64::new(0)),
            rdma_semaphore: Arc::new(tokio::sync::Semaphore::new(self.config.max_rdma_wr)),
            config: self.config,

            shutdown_initiated: Arc::new(AtomicBool::new(false)),
            shutdown_received: Arc::new(AtomicBool::new(false)),
            pending_sends: Arc::new(StdMutex::new(VecDeque::new())),
            context: self.context,
        }
    }
}

impl RdmaTransport {
    /// Create a new RDMA transport with default configuration.
    #[inline]
    pub fn new(context: Arc<RdmaContext>) -> io::Result<Self> {
        Self::with_config(context, TransportConfig::default())
    }

    /// Create a new RDMA transport with custom configuration.
    pub fn with_config(context: Arc<RdmaContext>, config: TransportConfig) -> io::Result<Self> {
        let cq: GenericCompletionQueue = context
            .ctx
            .create_cq_builder()
            .build()
            .map_err(|e| io::Error::other(e.to_string()))?
            .into();

        let qp = Self::build_queue_pair(&context, &cq, &config)?;

        Ok(TransportParts {
            context,
            cq,
            qp,
            config,
            cm_id: None,
        }
        .into_transport())
    }

    /// Build a queue pair with the given configuration.
    pub(crate) fn build_queue_pair(
        context: &RdmaContext,
        cq: &GenericCompletionQueue,
        config: &TransportConfig,
    ) -> io::Result<GenericQueuePair> {
        let desired_send_wr = DEFAULT_RECV_DEPTH.max(
            config
                .max_outstanding_sends
                .saturating_add(config.max_rdma_wr)
                .saturating_add(64),
        );

        let mut builder = context.pd.create_qp_builder();
        builder.setup_send_cq(cq.clone());
        builder.setup_recv_cq(cq.clone());
        builder.setup_qp_type(QueuePairType::ReliableConnection);
        builder
            .setup_max_send_wr((desired_send_wr.min(u32::MAX as usize)) as u32)
            .setup_max_send_sge(1)
            .setup_max_recv_wr(DEFAULT_RECV_DEPTH as u32)
            .setup_max_recv_sge(1);

        let qp = builder.build().map_err(|e| io::Error::other(e.to_string()))?;
        Ok(GenericQueuePair::Basic(qp))
    }

    /// Create a transport from an already-connected CM ID with custom configuration.
    pub(crate) fn from_connected_with_config(
        cm_id: Arc<Identifier>,
        context: Arc<RdmaContext>,
        cq: GenericCompletionQueue,
        qp: GenericQueuePair,
        config: TransportConfig,
    ) -> Self {
        TransportParts {
            context,
            cq,
            qp,
            config,
            cm_id: Some(cm_id),
        }
        .into_transport()
    }

    /// Set the maximum receive buffer size.
    #[inline]
    pub fn with_max_recv_bytes(mut self, max_recv_bytes: usize) -> Self {
        self.max_recv_bytes = max_recv_bytes;
        self
    }

    /// Allocate a new work request ID.
    #[inline]
    pub(crate) fn alloc_wr_id(&self) -> u64 {
        self.next_wr_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the receive buffer length (header + max payload).
    #[inline]
    fn recv_buf_len(&self) -> usize {
        4 + self.max_recv_bytes
    }

    /// Ensure the receive path is started.
    async fn ensure_started(&self) -> io::Result<()> {
        self.started
            .get_or_try_init(|| async { self.init_receive_path().await })
            .await
            .map(|_| ())
    }

    /// Initialize the receive path with pre-posted buffers and background completion task.
    async fn init_receive_path(&self) -> io::Result<()> {
        let mut wc_rx = {
            let mut guard = self.recv_wc_rx.lock().await;
            guard
                .take()
                .ok_or_else(|| io::Error::other("receive path already initialized"))?
        };

        let recv_len = self.recv_buf_len();
        let depth = self.recv_manager.target_depth;

        // Clone Arcs for the spawned task
        let qp = Arc::clone(&self.qp);
        let posted_recvs = Arc::clone(&self.posted_recvs);
        let context = Arc::clone(&self.context);
        let pool = Arc::clone(&self.pool);
        let next_wr_id = Arc::clone(&self.next_wr_id);
        let recv_manager = Arc::clone(&self.recv_manager);
        let flow_controller = Arc::clone(&self.flow_controller);
        let recv_not_acked = Arc::clone(&self.recv_not_acked);
        let buf_ack_batch = self.config.buf_ack_batch;
        let shutdown_received = Arc::clone(&self.shutdown_received);

        // Pre-post receive buffers (best-effort: continue with whatever we can post)
        let posted_cnt = self
            .pre_post_receive_buffers(&qp, &posted_recvs, &context, &pool, &next_wr_id, recv_len, depth)
            .await;

        if posted_cnt == 0 {
            *self.recv_wc_rx.lock().await = Some(wc_rx);
            return Err(io::Error::other("failed to post any initial receive buffers"));
        }

        // Spawn background task: process receive completions
        tokio::spawn(async move {
            while let Some(ev) = wc_rx.recv().await {
                handle_recv_completion(
                    ev,
                    &posted_recvs,
                    &recv_manager,
                    &qp,
                    &context,
                    &pool,
                    &next_wr_id,
                    &recv_not_acked,
                    &flow_controller,
                    recv_len,
                    buf_ack_batch,
                    &shutdown_received,
                )
                .await;
            }
        });

        Ok(())
    }

    /// Pre-post receive buffers to establish initial receive depth.
    async fn pre_post_receive_buffers(
        &self,
        qp: &Arc<Mutex<GenericQueuePair>>,
        posted_recvs: &Arc<DashMap<u64, PostedRecvBuf>>,
        context: &Arc<RdmaContext>,
        pool: &Arc<RdmaBufferPool>,
        next_wr_id: &Arc<AtomicU64>,
        recv_len: usize,
        depth: usize,
    ) -> usize {
        let mut posted_cnt = 0;
        for _ in 0..depth {
            let wr_id = next_wr_id.fetch_add(1, Ordering::Relaxed);
            let buf = match alloc_recv_buffer(context, pool, recv_len) {
                Ok(b) => b,
                Err(e) => {
                    warn!("failed to allocate initial recv buffer: {e} (posted={posted_cnt}/{depth})");
                    break;
                }
            };
            if let Err(e) = post_recv_wr(qp, posted_recvs, wr_id, buf, recv_len).await {
                warn!("failed to post initial recv buffer: {e} (posted={posted_cnt}/{depth})");
                break;
            }
            posted_cnt += 1;
        }
        posted_cnt
    }

    /// Wait for a completion event for the given work request ID.
    #[inline]
    pub fn wait_for_completion(&self, wr_id: u64) -> impl Future<Output = io::Result<Completion>> + '_ {
        make_completion_future(Arc::clone(&self.poller), wr_id)
    }

    /// Gracefully shutdown the transport connection.
    ///
    /// Sends a CLOSE message to the peer, waits for acknowledgment, drains CQ,
    /// and transitions QP to Error state if timeout is reached.
    pub async fn shutdown(&self) -> io::Result<()> {
        let config = ShutdownConfig::default();
        let result = graceful_shutdown(
            &self.qp,
            &self.next_wr_id,
            &self.poller,
            &self.shutdown_initiated,
            &self.shutdown_received,
            &config,
        )
        .await;

        match result {
            ShutdownResult::Graceful => {
                debug!("graceful shutdown completed");
            }
            ShutdownResult::ForcedError => {
                debug!("shutdown forced QP to Error state");
            }
            ShutdownResult::AlreadyShuttingDown => {
                debug!("shutdown already in progress");
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Transport for RdmaTransport {
    async fn send(&self, buf: Bytes) -> io::Result<()> {
        // Ensure background receive path is running (helps avoid RNRs under load)
        let _ = self.ensure_started().await;

        // 1. Acquire send credit (blocks if none available)
        let _guard = self.flow_controller.acquire().await;

        let wr_id = self.alloc_wr_id();

        // Frame: u32 length prefix (big endian) + payload
        let total_len = 4 + buf.len();

        // Prepare send buffer (prefer slab allocation to avoid registration)
        let (lkey, addr, len, keepalive) = self.prepare_send_buffer(&buf, total_len)?;

        // 2. Determine if this send should be signaled (batching)
        let sends_since_signal = self.send_not_signaled.fetch_add(1, Ordering::Relaxed);
        let should_signal = sends_since_signal + 1 >= self.config.send_signal_batch as u64;

        let flags = if should_signal {
            self.send_not_signaled.store(0, Ordering::Relaxed);
            WorkRequestFlags::Signaled
        } else {
            WorkRequestFlags::none()
        };

        // SAFETY-CRITICAL: Keepalive must be stored BEFORE posting the WR.
        //
        // If we were to post first and then panic (e.g. due to a poisoned mutex),
        // the keepalive would be dropped while the NIC may still DMA from it (UB).
        {
            // Treat poisoning as recoverable: we must not panic after posting a WR.
            let mut pending = self
                .pending_sends
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pending.push_back(PendingSend {
                wr_id,
                _keepalive: keepalive,
            });
        }

        // Post the work request. If posting fails, remove the keepalive entry to avoid leaks.
        {
            let mut qp = self.qp.lock().await;
            let mut guard = qp.start_post_send();
            let wr = guard.construct_wr(wr_id, flags);
            unsafe {
                wr.setup_send().setup_sge(lkey, addr, len);
            }
            if let Err(e) = guard.post() {
                let mut pending = self
                    .pending_sends
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(pos) = pending.iter().position(|p| p.wr_id == wr_id) {
                    pending.remove(pos);
                }
                return Err(io::Error::other(e.to_string()));
            }
        }

        // 3. Handle completion for signaled sends
        if should_signal {
            self.handle_signaled_completion(wr_id).await?;
        }

        Ok(())
    }

    async fn recv(&self) -> io::Result<Bytes> {
        self.ensure_started().await?;
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

impl RdmaTransport {
    /// Prepare send buffer, preferring slab allocation over dynamic registration.
    fn prepare_send_buffer(&self, buf: &Bytes, total_len: usize) -> io::Result<(u32, u64, u32, SendKeepalive)> {
        if let Some(mut chunk) = self.context.slab.alloc(total_len) {
            let slice = chunk.as_mut_slice();
            slice[0..4].copy_from_slice(&(buf.len() as u32).to_be_bytes());
            slice[4..4 + buf.len()].copy_from_slice(buf.as_ref());
            Ok((
                chunk.lkey(),
                chunk.as_ptr() as u64,
                total_len as u32,
                SendKeepalive::Slab(chunk),
            ))
        } else {
            let mut framed = BytesMut::with_capacity(total_len);
            framed.extend_from_slice(&(buf.len() as u32).to_be_bytes());
            framed.extend_from_slice(buf.as_ref());
            let mut mr = RdmaMr::register(&self.context, framed)
                .ok_or_else(|| io::Error::other("failed to register memory"))?;
            let lkey = mr.lkey();
            let addr = mr.as_mut_slice().as_ptr() as u64;
            let len = mr.as_slice().len() as u32;
            Ok((lkey, addr, len, SendKeepalive::Dynamic(mr)))
        }
    }

    /// Handle completion for a signaled send, cleaning up pending sends.
    async fn handle_signaled_completion(&self, wr_id: u64) -> io::Result<()> {
        let mut reaper = SignaledSendReaper {
            poller: Arc::clone(&self.poller),
            pending_sends: Arc::clone(&self.pending_sends),
            wr_id,
            done: false,
        };

        PostSendGuard {
            poller: &self.poller,
            wr_id,
        }
        .wait()
        .await?;

        // Clean up all pending sends up to (and including) this wr_id
        // (RC ordering guarantees earlier sends have completed)
        let mut pending = self.pending_sends.lock().expect("pending_sends poisoned");
        while let Some(front) = pending.front() {
            if front.wr_id <= wr_id {
                pending.pop_front();
            } else {
                break;
            }
        }
        reaper.done = true;

        Ok(())
    }
}
