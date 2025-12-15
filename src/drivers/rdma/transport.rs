use crate::transport::{Transport, BufferPool};
use crate::drivers::rdma::context::RdmaContext;
use crate::drivers::rdma::buffer::{RdmaBufferPool, RdmaMr};
use crate::drivers::rdma::poller::{Completion, CompletionEvent, RdmaPoller};
use crate::drivers::rdma::recv_pool::RecvBufferManager;
use crate::drivers::rdma::slab_allocator::SlabChunk;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use sideway::ibverbs::queue_pair::{
    GenericQueuePair, PostSendGuard as _, QueuePair, QueuePairType, SetScatterGatherEntry, WorkRequestFlags,
};
use sideway::ibverbs::completion::GenericCompletionQueue;
use sideway::ibverbs::completion::{WorkCompletionOperationType, WorkCompletionStatus};
use std::io;
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, Mutex, OnceCell};
use sideway::rdmacm::communication_manager::Identifier;
use tracing::{debug, warn};

const DEFAULT_MAX_RECV_BYTES: usize = 1024 * 1024; // 1 MiB payload max, plus 4-byte length header
const DEFAULT_RECV_DEPTH: usize = 512;

#[allow(dead_code)]
pub struct RdmaTransport {
    context: Arc<RdmaContext>,
    pool: Arc<RdmaBufferPool>,
    poller: Arc<RdmaPoller>,
    qp: Arc<Mutex<GenericQueuePair>>,
    cq: GenericCompletionQueue,
    next_wr_id: Arc<AtomicU64>,
    max_recv_bytes: usize,
    _cm_id: Option<Arc<Identifier>>,

    started: OnceCell<()>,
    recv_manager: Arc<RecvBufferManager>,
    recv_wc_rx: Mutex<Option<mpsc::UnboundedReceiver<CompletionEvent>>>,
    posted_recvs: Arc<DashMap<u64, PostedRecvBuf>>,
}

unsafe impl Send for RdmaTransport {}
unsafe impl Sync for RdmaTransport {}

#[allow(dead_code)]
enum SendKeepalive {
    Slab(SlabChunk),
    Dynamic(RdmaMr),
}

struct PostSendGuard<'a> {
    transport: &'a RdmaTransport,
    wr_id: u64,
    _keepalive: SendKeepalive,
}

impl<'a> PostSendGuard<'a> {
    async fn wait(self) -> io::Result<()> {
        let c = self.transport.wait_for_completion(self.wr_id).await?;
        if c.status != WorkCompletionStatus::Success {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("RDMA send failed: status={:?} opcode={:?}", c.status, c.opcode),
            ));
        }
        Ok(())
    }
}

enum PostedRecvBuf {
    Slab(SlabChunk),
    Dynamic(RdmaMr),
}

impl PostedRecvBuf {
    fn sge(&self, recv_len: usize) -> (u32, u64, u32) {
        match self {
            PostedRecvBuf::Slab(chunk) => (chunk.lkey(), chunk.as_ptr() as u64, recv_len as u32),
            PostedRecvBuf::Dynamic(mr) => (mr.lkey(), mr.buf.as_ptr() as u64, recv_len as u32),
        }
    }
}

struct SlabPayloadOwner {
    chunk: SlabChunk,
    offset: usize,
    len: usize,
}

impl AsRef<[u8]> for SlabPayloadOwner {
    fn as_ref(&self) -> &[u8] {
        &self.chunk.as_slice()[self.offset..self.offset + self.len]
    }
}

impl RdmaTransport {
    pub fn new(context: Arc<RdmaContext>) -> io::Result<Self> {
        let cq: GenericCompletionQueue = context.ctx.create_cq_builder()
            .build()
            .map_err(|e| io::Error::other(e.to_string()))?
            .into();

        let mut builder = context.pd.create_qp_builder();
        builder.setup_send_cq(cq.clone());
        builder.setup_recv_cq(cq.clone());
        builder.setup_qp_type(QueuePairType::ReliableConnection);
        // Ensure the QP supports the default receive depth.
        builder
            .setup_max_send_wr(DEFAULT_RECV_DEPTH as u32)
            .setup_max_send_sge(1)
            .setup_max_recv_wr(DEFAULT_RECV_DEPTH as u32)
            .setup_max_recv_sge(1);

        let qp = builder.build()
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
        })
    }

    pub(crate) fn from_connected(
        cm_id: Arc<Identifier>,
        context: Arc<RdmaContext>,
        cq: GenericCompletionQueue,
        qp: GenericQueuePair,
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
        }
    }

    pub fn with_max_recv_bytes(mut self, max_recv_bytes: usize) -> Self {
        self.max_recv_bytes = max_recv_bytes;
        self
    }

    fn alloc_wr_id(&self) -> u64 {
        self.next_wr_id.fetch_add(1, Ordering::Relaxed)
    }

    fn recv_buf_len(&self) -> usize {
        4 + self.max_recv_bytes
    }

    async fn ensure_started(&self) -> io::Result<()> {
        self.started
            .get_or_try_init(|| async { self.init_receive_path().await })
            .await
            .map(|_| ())
    }

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

        // Helper: allocate a posted receive buffer (slab preferred, dynamic fallback).
        fn alloc_posted_recv(
            context: &Arc<RdmaContext>,
            pool: &Arc<RdmaBufferPool>,
            recv_len: usize,
        ) -> io::Result<PostedRecvBuf> {
            if let Some(chunk) = context.slab.alloc(recv_len) {
                return Ok(PostedRecvBuf::Slab(chunk));
            }
            let mut buf = pool.alloc(recv_len);
            buf.resize(recv_len, 0);
            let mr = RdmaMr::register(context, buf)
                .ok_or_else(|| io::Error::other("Failed to register recv memory"))?;
            Ok(PostedRecvBuf::Dynamic(mr))
        }

        // Helper: post a recv WR and record its buffer before posting (to avoid races).
        async fn post_recv_wr(
            qp: &Arc<Mutex<GenericQueuePair>>,
            posted_recvs: &Arc<DashMap<u64, PostedRecvBuf>>,
            wr_id: u64,
            buf: PostedRecvBuf,
            recv_len: usize,
        ) -> io::Result<()> {
            let (lkey, addr, len) = buf.sge(recv_len);
            posted_recvs.insert(wr_id, buf);
            let res = async {
                let mut qp = qp.lock().await;
                let mut guard = qp.start_post_recv();
                unsafe {
                    guard.construct_wr(wr_id).setup_sge(lkey, addr, len);
                }
                guard
                    .post()
                    .map_err(|e| io::Error::other(e.to_string()))
            }
            .await;
            if res.is_err() {
                posted_recvs.remove(&wr_id);
            }
            res
        }

        // Pre-post receive buffers to avoid RNR and stop-and-wait behavior.
        // If we fail before spawning the consumer task, put the receiver back so we can retry.
        for _ in 0..depth {
            let wr_id = next_wr_id.fetch_add(1, Ordering::Relaxed);
            let buf = alloc_posted_recv(&context, &pool, recv_len)?;
            if let Err(e) = post_recv_wr(&qp, &posted_recvs, wr_id, buf, recv_len).await {
                *self.recv_wc_rx.lock().await = Some(wc_rx);
                return Err(e);
            }
        }

        // Spawn background task: consume receive completions, queue messages, repost receives.
        tokio::spawn(async move {
            loop {
                let ev = match wc_rx.recv().await {
                    Some(ev) => ev,
                    None => break,
                };

                let wr_id = ev.wr_id;
                let c = ev.completion;

                let posted = posted_recvs.remove(&wr_id).map(|(_, b)| b);
                let Some(posted) = posted else {
                    debug!("recv completion for unknown wr_id={wr_id}");
                    continue;
                };

                if c.status != WorkCompletionStatus::Success {
                    warn!("RDMA recv failed: status={:?} opcode={:?} wr_id={}", c.status, c.opcode, wr_id);
                } else if c.opcode != WorkCompletionOperationType::Receive
                    && c.opcode != WorkCompletionOperationType::ReceiveWithImmediate
                {
                    warn!("unexpected recv completion opcode={:?} wr_id={}", c.opcode, wr_id);
                } else {
                    let received = c.byte_len as usize;
                    if received < 4 {
                        warn!("RDMA recv too short: {} bytes", received);
                    } else {
                        match posted {
                            PostedRecvBuf::Slab(chunk) => {
                                let frame = &chunk.as_slice()[..received.min(recv_len)];
                                let msg_len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
                                if msg_len <= frame.len().saturating_sub(4) {
                                    recv_manager.push(Bytes::from_owner(SlabPayloadOwner {
                                        chunk,
                                        offset: 4,
                                        len: msg_len,
                                    }));
                                } else {
                                    warn!("RDMA recv bad framing: msg_len={} received={}", msg_len, received);
                                }
                            }
                            PostedRecvBuf::Dynamic(mr) => {
                                let RdmaMr { mr: _mr, mut buf } = mr;
                                drop(_mr);
                                buf.truncate(received.min(recv_len));
                                let msg_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                                if msg_len <= buf.len().saturating_sub(4) {
                                    let mut payload = buf.split_off(4);
                                    payload.truncate(msg_len);
                                    recv_manager.push(payload.freeze());
                                } else {
                                    warn!("RDMA recv bad framing: msg_len={} received={}", msg_len, received);
                                }
                            }
                        }
                    }
                }

                // Always try to repost to maintain receive depth.
                let new_wr_id = next_wr_id.fetch_add(1, Ordering::Relaxed);
                let new_buf = match alloc_posted_recv(&context, &pool, recv_len) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("failed to allocate recv buffer for repost: {e}");
                        continue;
                    }
                };
                if let Err(e) = post_recv_wr(&qp, &posted_recvs, new_wr_id, new_buf, recv_len).await {
                    warn!("failed to repost recv buffer: {e}");
                }
            }
        });

        Ok(())
    }

    pub fn wait_for_completion(&self, wr_id: u64) -> impl Future<Output = io::Result<Completion>> + '_ {
        struct CompletionFuture<'a> {
            poller: &'a RdmaPoller,
            wr_id: u64,
        }

        impl<'a> Future for CompletionFuture<'a> {
            type Output = io::Result<Completion>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                if let Some(c) = this.poller.take_completion(this.wr_id) {
                    return Poll::Ready(Ok(c));
                }
                this.poller.register(this.wr_id, cx.waker().clone());
                Poll::Pending
            }
        }

        CompletionFuture {
            poller: &self.poller,
            wr_id,
        }
    }
}

#[async_trait]
impl Transport for RdmaTransport {
    async fn send(&self, buf: Bytes) -> io::Result<()> {
        // Ensure the background receive path is running (helps avoid RNRs under load).
        let _ = self.ensure_started().await;

        let wr_id = self.alloc_wr_id();

        // Frame like TCP transport: u32 length prefix (big endian) + payload.
        let total_len = 4 + buf.len();

        let (lkey, addr, len, keepalive) = if let Some(mut chunk) = self.context.slab.alloc(total_len) {
            let slice = chunk.as_mut_slice();
            slice[0..4].copy_from_slice(&(buf.len() as u32).to_be_bytes());
            slice[4..4 + buf.len()].copy_from_slice(buf.as_ref());
            (chunk.lkey(), chunk.as_ptr() as u64, total_len as u32, SendKeepalive::Slab(chunk))
        } else {
            let mut framed = BytesMut::with_capacity(total_len);
            framed.extend_from_slice(&(buf.len() as u32).to_be_bytes());
            framed.extend_from_slice(buf.as_ref());
            let mr = RdmaMr::register(&self.context, framed)
                .ok_or_else(|| io::Error::other("Failed to register memory"))?;
            let lkey = mr.lkey();
            let addr = mr.buf.as_ptr() as u64;
            let len = mr.buf.len() as u32;
            (lkey, addr, len, SendKeepalive::Dynamic(mr))
        };

        {
            let mut qp = self.qp.lock().await;
            let mut guard = qp.start_post_send();
            let wr = guard.construct_wr(wr_id, WorkRequestFlags::Signaled);
            unsafe {
                wr.setup_send().setup_sge(lkey, addr, len);
            }
            guard
                .post()
                .map_err(|e| io::Error::other(e.to_string()))?;
        }

        PostSendGuard {
            transport: self,
            wr_id,
            _keepalive: keepalive,
        }
        .wait()
        .await
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
