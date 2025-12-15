//! RDMA Transport implementation.
//!
//! This module provides a high-performance RDMA-based transport layer with:
//! - Credit-based flow control
//! - Signal batching for reduced completion overhead
//! - Slab allocation for efficient buffer management
//! - One-sided RDMA READ/WRITE operations
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

mod completion;
pub mod config;
mod flow_control;
pub mod protocol;
mod rdma_ops;
mod recv;
mod recv_pool;
mod send;
mod types;

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
use send::{send_close_msg, PostSendGuard, SignaledSendReaper};
use types::{PendingSend, PostedRecvBuf, SendKeepalive};

/// High-performance RDMA transport with credit-based flow control and signal batching.
#[allow(dead_code)]
pub struct RdmaTransport {
    pub(crate) context: Arc<RdmaContext>,
    pub(crate) pool: Arc<RdmaBufferPool>,
    pub(crate) poller: Arc<RdmaPoller>,
    pub(crate) qp: Arc<Mutex<GenericQueuePair>>,
    cq: GenericCompletionQueue,
    pub(crate) next_wr_id: Arc<AtomicU64>,
    max_recv_bytes: usize,
    _cm_id: Option<Arc<Identifier>>,

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

unsafe impl Send for RdmaTransport {}
unsafe impl Sync for RdmaTransport {}

impl RdmaTransport {
    /// Create a new RDMA transport with default configuration.
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

        let mut builder = context.pd.create_qp_builder();
        builder.setup_send_cq(cq.clone());
        builder.setup_recv_cq(cq.clone());
        builder.setup_qp_type(QueuePairType::ReliableConnection);
        // Ensure the QP supports the default receive depth.
        let desired_send_wr = DEFAULT_RECV_DEPTH.max(
            config
                .max_outstanding_sends
                .saturating_add(config.max_rdma_wr)
                .saturating_add(64),
        );
        builder
            .setup_max_send_wr((desired_send_wr.min(u32::MAX as usize)) as u32)
            .setup_max_send_sge(1)
            .setup_max_recv_wr(DEFAULT_RECV_DEPTH as u32)
            .setup_max_recv_sge(1);

        let qp = builder
            .build()
            .map_err(|e| io::Error::other(e.to_string()))?;

        let (recv_wc_tx, recv_wc_rx) = mpsc::unbounded_channel();

        Ok(Self {
            pool: Arc::new(RdmaBufferPool::new(context.clone())),
            context,
            poller: Arc::new(RdmaPoller::new_with_recv(cq.clone(), Some(recv_wc_tx))),
            qp: Arc::new(Mutex::new(GenericQueuePair::Basic(qp))),
            cq,
            next_wr_id: Arc::new(AtomicU64::new(1)),
            max_recv_bytes: DEFAULT_MAX_RECV_BYTES,
            _cm_id: None,

            started: OnceCell::new(),
            recv_manager: RecvBufferManager::new(DEFAULT_RECV_DEPTH),
            recv_wc_rx: Mutex::new(Some(recv_wc_rx)),
            posted_recvs: Arc::new(DashMap::new()),

            flow_controller: Arc::new(FlowController::new(config.max_outstanding_sends)),
            recv_not_acked: Arc::new(AtomicU64::new(0)),
            send_not_signaled: Arc::new(AtomicU64::new(0)),
            rdma_semaphore: Arc::new(tokio::sync::Semaphore::new(config.max_rdma_wr)),
            config,

            shutdown_initiated: Arc::new(AtomicBool::new(false)),
            shutdown_received: Arc::new(AtomicBool::new(false)),
            pending_sends: Arc::new(StdMutex::new(VecDeque::new())),
        })
    }

    /// Create a transport from an already-connected CM ID.
    pub(crate) fn from_connected(
        cm_id: Arc<Identifier>,
        context: Arc<RdmaContext>,
        cq: GenericCompletionQueue,
        qp: GenericQueuePair,
    ) -> Self {
        Self::from_connected_with_config(cm_id, context, cq, qp, TransportConfig::default())
    }

    /// Create a transport from an already-connected CM ID with custom configuration.
    pub(crate) fn from_connected_with_config(
        cm_id: Arc<Identifier>,
        context: Arc<RdmaContext>,
        cq: GenericCompletionQueue,
        qp: GenericQueuePair,
        config: TransportConfig,
    ) -> Self {
        let (recv_wc_tx, recv_wc_rx) = mpsc::unbounded_channel();
        Self {
            pool: Arc::new(RdmaBufferPool::new(context.clone())),
            poller: Arc::new(RdmaPoller::new_with_recv(cq.clone(), Some(recv_wc_tx))),
            qp: Arc::new(Mutex::new(qp)),
            cq,
            next_wr_id: Arc::new(AtomicU64::new(1)),
            max_recv_bytes: DEFAULT_MAX_RECV_BYTES,
            context,
            _cm_id: Some(cm_id),

            started: OnceCell::new(),
            recv_manager: RecvBufferManager::new(DEFAULT_RECV_DEPTH),
            recv_wc_rx: Mutex::new(Some(recv_wc_rx)),
            posted_recvs: Arc::new(DashMap::new()),

            flow_controller: Arc::new(FlowController::new(config.max_outstanding_sends)),
            recv_not_acked: Arc::new(AtomicU64::new(0)),
            send_not_signaled: Arc::new(AtomicU64::new(0)),
            rdma_semaphore: Arc::new(tokio::sync::Semaphore::new(config.max_rdma_wr)),
            config,

            shutdown_initiated: Arc::new(AtomicBool::new(false)),
            shutdown_received: Arc::new(AtomicBool::new(false)),
            pending_sends: Arc::new(StdMutex::new(VecDeque::new())),
        }
    }

    /// Set the maximum receive buffer size.
    pub fn with_max_recv_bytes(mut self, max_recv_bytes: usize) -> Self {
        self.max_recv_bytes = max_recv_bytes;
        self
    }

    /// Allocate a new work request ID.
    pub(crate) fn alloc_wr_id(&self) -> u64 {
        self.next_wr_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the receive buffer length (header + max payload).
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

        // Pre-post receive buffers to establish initial receive depth.
        //
        // We treat this as best-effort: if we fail part-way through, we still start the receive
        // completion task with the buffers that *were* posted, rather than returning an error and
        // leaving the QP in a half-initialized state.
        let mut posted_cnt: usize = 0;
        for _ in 0..depth {
            let wr_id = next_wr_id.fetch_add(1, Ordering::Relaxed);
            let buf = match alloc_recv_buffer(&context, &pool, recv_len) {
                Ok(b) => b,
                Err(e) => {
                    warn!("failed to allocate initial recv buffer: {e} (posted={posted_cnt}/{depth})");
                    break;
                }
            };
            if let Err(e) = post_recv_wr(&qp, &posted_recvs, wr_id, buf, recv_len).await {
                warn!("failed to post initial recv buffer: {e} (posted={posted_cnt}/{depth})");
                break;
            }
            posted_cnt += 1;
        }

        if posted_cnt == 0 {
            *self.recv_wc_rx.lock().await = Some(wc_rx);
            return Err(io::Error::other(
                "failed to post any initial receive buffers",
            ));
        }

        // Spawn background task: process receive completions
        tokio::spawn(async move {
            loop {
                let ev = match wc_rx.recv().await {
                    Some(ev) => ev,
                    None => break,
                };

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

    /// Wait for a completion event for the given work request ID.
    pub fn wait_for_completion(&self, wr_id: u64) -> impl Future<Output = io::Result<Completion>> + '_ {
        make_completion_future(Arc::clone(&self.poller), wr_id)
    }

    /// Gracefully shutdown the transport connection.
    ///
    /// Sends a CLOSE message to the peer and waits for acknowledgment (or timeout).
    /// This ensures both sides cleanly terminate the connection.
    pub async fn shutdown(&self) -> io::Result<()> {
        // Mark shutdown as initiated
        if self
            .shutdown_initiated
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            // Already shutting down
            return Ok(());
        }

        debug!("initiating graceful shutdown");

        // Send CLOSE message to peer
        if let Err(e) = send_close_msg(&self.qp, &self.next_wr_id).await {
            warn!("failed to send CLOSE message: {}", e);
            // Continue with shutdown anyway
        }

        // Wait for peer CLOSE or timeout
        let timeout = tokio::time::Duration::from_millis(200);
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            if self
                .shutdown_received
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                debug!("received CLOSE from peer, shutdown complete");
                return Ok(());
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        debug!("shutdown timeout, proceeding anyway");
        Ok(())
    }
}

#[async_trait]
impl Transport for RdmaTransport {
    async fn send(&self, buf: Bytes) -> io::Result<()> {
        // Ensure the background receive path is running (helps avoid RNRs under load).
        let _ = self.ensure_started().await;

        // 1. Acquire send credit (blocks if none available)
        let _guard = self.flow_controller.acquire().await;

        let wr_id = self.alloc_wr_id();

        // Frame like TCP transport: u32 length prefix (big endian) + payload.
        let total_len = 4 + buf.len();

        let (lkey, addr, len, keepalive) = if let Some(mut chunk) = self.context.slab.alloc(total_len)
        {
            let slice = chunk.as_mut_slice();
            slice[0..4].copy_from_slice(&(buf.len() as u32).to_be_bytes());
            slice[4..4 + buf.len()].copy_from_slice(buf.as_ref());
            (
                chunk.lkey(),
                chunk.as_ptr() as u64,
                total_len as u32,
                SendKeepalive::Slab(chunk),
            )
        } else {
            let mut framed = BytesMut::with_capacity(total_len);
            framed.extend_from_slice(&(buf.len() as u32).to_be_bytes());
            framed.extend_from_slice(buf.as_ref());
            let mut mr = RdmaMr::register(&self.context, framed)
                .ok_or_else(|| io::Error::other("Failed to register memory"))?;
            let lkey = mr.lkey();
            let addr = mr.as_mut_slice().as_ptr() as u64;
            let len = mr.as_slice().len() as u32;
            (lkey, addr, len, SendKeepalive::Dynamic(mr))
        };

        // 2. Determine if this send should be signaled (batching)
        let sends_since_signal = self.send_not_signaled.fetch_add(1, Ordering::Relaxed);
        let should_signal = sends_since_signal + 1 >= self.config.send_signal_batch as u64;

        let flags = if should_signal {
            self.send_not_signaled.store(0, Ordering::Relaxed);
            WorkRequestFlags::Signaled
        } else {
            WorkRequestFlags::none()
        };

        // Pre-allocate the pending send slot to ensure the keepalive is stored
        // BEFORE we post the work request. This prevents a race where a panic/cancellation
        // drops the keepalive while the NIC is accessing the memory.
        {
            let mut pending = self.pending_sends.lock().expect("pending_sends poisoned");
            pending.push_back(PendingSend {
                wr_id,
                _keepalive: keepalive,
            });
        }

        {
            let mut qp = self.qp.lock().await;
            let mut guard = qp.start_post_send();
            let wr = guard.construct_wr(wr_id, flags);
            unsafe {
                wr.setup_send().setup_sge(lkey, addr, len);
            }
            // If posting fails, we must remove the pending send to avoid leaks/confusion.
            if let Err(e) = guard.post() {
                let mut pending = self.pending_sends.lock().expect("pending_sends poisoned");
                // We just pushed to the back, so remove the back.
                // Verify it's the right one (though in this scope it should be).
                if let Some(back) = pending.back() {
                    if back.wr_id == wr_id {
                        pending.pop_back();
                    }
                }
                return Err(io::Error::other(e.to_string()));
            }
        }

        // 3. Handle completion based on signaling
        if should_signal {
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

            // Clean up all pending sends up to (and including) this wr_id (RC ordering guarantees completion).
            let mut pending = self.pending_sends.lock().expect("pending_sends poisoned");
            while let Some(front) = pending.front() {
                if front.wr_id <= wr_id {
                    pending.pop_front();
                } else {
                    break;
                }
            }
            reaper.done = true;
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

    fn alloc_buf(&self, size: usize) -> BytesMut {
        self.pool.alloc(size)
    }
}
