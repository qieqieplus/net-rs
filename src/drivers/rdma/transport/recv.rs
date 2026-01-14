use super::flow_control::FlowController;
use super::protocol::{decode_credit_imm, is_close_imm, MSG_HEADER_SIZE};
use super::recv_pool::RecvBufferManager;
use super::send::send_credit_ack;
use super::TransportInner;
use super::types::{PostedRecvBuf, SlabPayloadOwner};
use crate::drivers::rdma::buffer::{RdmaBufferPool, RdmaMr};
use crate::drivers::rdma::context::RdmaContext;
use crate::drivers::rdma::poller::CompletionEvent;
use crate::transport::BufferPool;
use bytes::Bytes;
use sideway::ibverbs::completion::{WorkCompletionOperationType, WorkCompletionStatus};
use sideway::ibverbs::queue_pair::{GenericQueuePair, QueuePair, SetScatterGatherEntry};
use rustc_hash::FxHashMap;
use std::io;
use std::rc::Rc;
use std::cell::{Cell, RefCell};
use std::sync::Arc;
use tracing::{debug, warn};

/// Maximum number of receive buffers to post in a single batch (to avoid starving the loop).
const MAX_POST_RECV_BATCH: usize = 16;

/// Context for receive completion handling.
#[allow(clippy::too_many_arguments)]
pub(crate) struct RecvContext {
    pub inner: Rc<TransportInner>,
    pub posted_recvs: Rc<RefCell<FxHashMap<u64, PostedRecvBuf>>>,
    pub recv_manager: Rc<RecvBufferManager>,
    pub context: Arc<RdmaContext>,
    pub pool: Arc<RdmaBufferPool>,
    pub flow_controller: FlowController,
    pub recv_len: usize,
    pub buf_ack_batch: usize,
}

/// Allocate a receive buffer, preferring slab allocation over dynamic.
pub(crate) fn alloc_recv_buffer(
    context: &Arc<RdmaContext>,
    pool: &Arc<RdmaBufferPool>,
    recv_len: usize,
) -> io::Result<PostedRecvBuf> {
    if let Some(chunk) = context.slab.alloc(recv_len) {
        return Ok(PostedRecvBuf::Slab(chunk));
    }
    let mut buf = pool.alloc(recv_len);
    unsafe {
        buf.set_len(recv_len);
    }
    let mr = RdmaMr::register(context, buf)
        .ok_or_else(|| io::Error::other("Failed to register recv memory"))?;
    Ok(PostedRecvBuf::Dynamic(mr))
}

/// Post a receive work request to the queue pair.
pub(crate) async fn post_recv_wr(
    qp: &RefCell<GenericQueuePair>,
    posted_recvs: &Rc<RefCell<FxHashMap<u64, PostedRecvBuf>>>,
    wr_id: u64,
    buf: PostedRecvBuf,
    recv_len: usize,
) -> io::Result<()> {
    let (lkey, addr, len) = buf.sge(recv_len);
    posted_recvs.borrow_mut().insert(wr_id, buf);

    // Synchronous borrow and post
    let res = {
        let mut qp = qp.borrow_mut();
        let mut guard = qp.start_post_recv();
        unsafe {
            guard.construct_wr(wr_id).setup_sge(lkey, addr, len);
        }
        guard.post().map_err(|e| io::Error::other(e.to_string()))
    };

    if res.is_err() {
        posted_recvs.borrow_mut().remove(&wr_id);
    }
    res
}

/// Process a single receive completion event.
pub(crate) async fn handle_recv_completion(ev: CompletionEvent, ctx: &RecvContext) {
    let wr_id = ev.wr_id;
    let c = ev.completion;

    // Retrieve posted buffer
    let posted = ctx.posted_recvs.borrow_mut().remove(&wr_id);
    let Some(posted) = posted else {
        debug!("recv completion for unknown wr_id={wr_id}");
        return;
    };

    // Check for failed completions first
    if c.status != WorkCompletionStatus::Success {
        warn!(
            "RDMA recv failed: status={:?} opcode={:?} wr_id={}",
            c.status, c.opcode, wr_id
        );
        repost_buffer_with_retry(ctx, posted).await;
        return;
    }

    // 1. Control Messages
    if c.opcode == WorkCompletionOperationType::ReceiveWithImmediate {
        if let Some(imm) = c.imm_data {
            if is_close_imm(imm) {
                debug!("received CLOSE message from peer");
                ctx.inner.shutdown_received.set(true);
            } else if let Some(credits) = decode_credit_imm(imm) {
                if credits > 0 {
                    ctx.flow_controller.release(credits as usize);
                    debug!("received credit ACK: {} credits", credits);
                }
            } else {
                warn!("unknown immediate data: {:#010x} (wr_id={})", imm, wr_id);
            }
        } else {
            warn!(
                "ReceiveWithImmediate completion without imm_data (wr_id={})",
                wr_id
            );
        }
        repost_buffer_with_retry(ctx, posted).await;
        return;
    }

    // 2. Data/Shutdown
    if c.byte_len == 0 {
        debug!("received 0-byte message (likely degraded CLOSE), treating as shutdown signal");
        ctx.inner.shutdown_received.set(true);
        repost_buffer_with_retry(ctx, posted).await;
        return;
    }

    // Extract payload from data message
    extract_received_message(posted, c.byte_len as usize, ctx.recv_len, &ctx.recv_manager);

    // 3. Maintenance
    repost_new_buffer_with_retry(ctx).await;
    top_up_receive_queue(ctx).await;
    maybe_send_credit_ack(&ctx.inner.recv_not_acked, ctx.buf_ack_batch, &ctx.inner.qp, &ctx.inner.next_wr_id).await;
}

/// Repost an existing buffer with retry logic.
async fn repost_buffer_with_retry(ctx: &RecvContext, posted: PostedRecvBuf) {
    let new_wr_id = {
        let id = ctx.inner.next_wr_id.get();
        ctx.inner.next_wr_id.set(id.wrapping_add(1));
        id
    };
    if let Err(e) = post_recv_wr(&ctx.inner.qp, &ctx.posted_recvs, new_wr_id, posted, ctx.recv_len).await {
        warn!("failed to repost recv buffer (will retry with fresh buffer): {e}");
        repost_new_buffer_with_retry(ctx).await;
    }
}

/// Allocate and post a new receive buffer with retry logic.
async fn repost_new_buffer_with_retry(ctx: &RecvContext) {
    let mut backoff_ms: u64 = 0;
    loop {
        let new_wr_id = {
            let id = ctx.inner.next_wr_id.get();
            ctx.inner.next_wr_id.set(id.wrapping_add(1));
            id
        };
        match alloc_recv_buffer(&ctx.context, &ctx.pool, ctx.recv_len) {
            Ok(buf) => match post_recv_wr(&ctx.inner.qp, &ctx.posted_recvs, new_wr_id, buf, ctx.recv_len).await {
                Ok(()) => break,
                Err(e) => {
                    warn!("failed to repost recv buffer (will retry): {e}");
                }
            },
            Err(e) => {
                warn!("failed to allocate recv buffer for repost (will retry): {e}");
            }
        }
        backoff_ms = if backoff_ms == 0 {
            1
        } else {
            (backoff_ms * 2).min(50)
        };
        monoio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
    }
}

/// Top up the receive queue to the target depth.
async fn top_up_receive_queue(ctx: &RecvContext) {
    let target_depth = ctx.recv_manager.target_depth;
    let mut extra_posts: usize = 0;
    while ctx.posted_recvs.borrow().len() < target_depth && extra_posts < MAX_POST_RECV_BATCH {
        let wr_id = {
            let id = ctx.inner.next_wr_id.get();
            ctx.inner.next_wr_id.set(id.wrapping_add(1));
            id
        };
        let buf = match alloc_recv_buffer(&ctx.context, &ctx.pool, ctx.recv_len) {
            Ok(b) => b,
            Err(e) => {
                warn!("failed to allocate recv buffer for depth top-up: {e}");
                break;
            }
        };
        match post_recv_wr(&ctx.inner.qp, &ctx.posted_recvs, wr_id, buf, ctx.recv_len).await {
            Ok(()) => extra_posts += 1,
            Err(e) => {
                warn!("failed to post recv buffer for depth top-up: {e}");
                break;
            }
        }
    }
}

/// Extract the received message from the buffer and queue it.
pub(crate) fn extract_received_message(
    posted: PostedRecvBuf,
    received: usize,
    recv_len: usize,
    recv_manager: &Rc<RecvBufferManager>,
) {
    if received < MSG_HEADER_SIZE {
        warn!("RDMA recv too short: {} bytes", received);
        return;
    }

    match posted {
        PostedRecvBuf::Slab(chunk) => {
            let frame = &chunk.as_slice()[..received.min(recv_len)];
            // Safety: Checked length above
            let msg_len = u32::from_be_bytes(frame[..MSG_HEADER_SIZE].try_into().unwrap()) as usize;
            if msg_len <= frame.len().saturating_sub(MSG_HEADER_SIZE) {
                recv_manager.push(Bytes::from_owner(SlabPayloadOwner {
                    chunk,
                    offset: MSG_HEADER_SIZE,
                    len: msg_len,
                }));
            } else {
                warn!(
                    "RDMA recv bad framing: msg_len={} received={}",
                    msg_len, received
                );
            }
        }
        PostedRecvBuf::Dynamic(mr) => {
            let mut buf = mr.into_inner();
            buf.truncate(received.min(recv_len));
            // Safety: Checked length above
            let msg_len = u32::from_be_bytes(buf[..MSG_HEADER_SIZE].try_into().unwrap()) as usize;
            if msg_len <= buf.len().saturating_sub(MSG_HEADER_SIZE) {
                let mut payload = buf.split_off(MSG_HEADER_SIZE);
                payload.truncate(msg_len);
                recv_manager.push(payload.freeze());
            } else {
                warn!(
                    "RDMA recv bad framing: msg_len={} received={}",
                    msg_len, received
                );
            }
        }
    }
}

/// Send credit ACK if enough receives have accumulated.
pub(crate) async fn maybe_send_credit_ack(
    recv_not_acked: &Cell<u64>,
    buf_ack_batch: usize,
    qp: &RefCell<GenericQueuePair>,
    next_wr_id: &Cell<u64>,
) {
    let acked = recv_not_acked.get() + 1;
    recv_not_acked.set(acked);
    if acked as usize >= buf_ack_batch {
        // Reset counter and send credit ACK
        recv_not_acked.set(0);
        let credits_to_ack = acked as u32;
        if credits_to_ack > 0 {
            let mut remaining = credits_to_ack;
            while remaining > 0 {
                let chunk = remaining.min(u16::MAX as u32);
                if let Err(e) = send_credit_ack(qp, next_wr_id, chunk).await {
                    warn!("failed to post credit ACK: {}", e);
                    // Return the remaining credits (including this chunk) to counter so we retry next time.
                    recv_not_acked.set(recv_not_acked.get() + remaining as u64);
                    break;
                }
                remaining -= chunk;
            }
        }
    }
}

/// Pre-post receive buffers to establish initial receive depth.
pub(crate) async fn pre_post_receive_buffers(
    qp: &RefCell<GenericQueuePair>,
    posted_recvs: &Rc<RefCell<FxHashMap<u64, PostedRecvBuf>>>,
    context: &Arc<RdmaContext>,
    pool: &Arc<RdmaBufferPool>,
    next_wr_id: &Cell<u64>,
    recv_len: usize,
    depth: usize,
) -> usize {
    let mut posted_cnt = 0;
    for _ in 0..depth {
        let wr_id = {
            let id = next_wr_id.get();
            next_wr_id.set(id.wrapping_add(1));
            id
        };
        let buf = match alloc_recv_buffer(context, pool, recv_len) {
            Ok(b) => b,
            Err(e) => {
                warn!("failed to allocate initial recv buffer: {e} (posted={posted_cnt}/{depth})");
                break;
            }
        };
        // post_recv_wr handles synchronous borrow internally
        if let Err(e) = post_recv_wr(qp, posted_recvs, wr_id, buf, recv_len).await {
            warn!("failed to post initial recv buffer: {e} (posted={posted_cnt}/{depth})");
            break;
        }
        posted_cnt += 1;
    }
    posted_cnt
}
