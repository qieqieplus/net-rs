//! Receive path helpers and logic.

use super::flow_control::FlowController;
use super::protocol::{decode_credit_imm, is_close_imm};
use super::recv_pool::RecvBufferManager;
use super::send::send_credit_ack;
use super::types::{PostedRecvBuf, SlabPayloadOwner};
use crate::drivers::rdma::buffer::{RdmaBufferPool, RdmaMr};
use crate::drivers::rdma::context::RdmaContext;
use crate::drivers::rdma::poller::CompletionEvent;
use crate::transport::BufferPool;
use bytes::Bytes;
use dashmap::DashMap;
use sideway::ibverbs::completion::{WorkCompletionOperationType, WorkCompletionStatus};
use sideway::ibverbs::queue_pair::{GenericQueuePair, QueuePair, SetScatterGatherEntry};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

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
    // Avoid zeroing: the NIC will overwrite the receive buffer.
    // Safety: we only read the bytes reported by the completion (and we guard against short reads).
    unsafe {
        buf.set_len(recv_len);
    }
    let mr = RdmaMr::register(context, buf)
        .ok_or_else(|| io::Error::other("Failed to register recv memory"))?;
    Ok(PostedRecvBuf::Dynamic(mr))
}

/// Post a receive work request to the queue pair.
pub(crate) async fn post_recv_wr(
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
        guard.post().map_err(|e| io::Error::other(e.to_string()))
    }
    .await;

    if res.is_err() {
        posted_recvs.remove(&wr_id);
    }
    res
}

/// Process a single receive completion event.
pub(crate) async fn handle_recv_completion(
    ev: CompletionEvent,
    posted_recvs: &Arc<DashMap<u64, PostedRecvBuf>>,
    recv_manager: &Arc<RecvBufferManager>,
    qp: &Arc<Mutex<GenericQueuePair>>,
    context: &Arc<RdmaContext>,
    pool: &Arc<RdmaBufferPool>,
    next_wr_id: &Arc<AtomicU64>,
    recv_not_acked: &Arc<AtomicU64>,
    flow_controller: &FlowController,
    recv_len: usize,
    buf_ack_batch: usize,
    shutdown_received: &Arc<AtomicBool>,
) {
    let wr_id = ev.wr_id;
    let c = ev.completion;

    // Retrieve posted buffer
    let posted = posted_recvs.remove(&wr_id).map(|(_, b)| b);
    let Some(posted) = posted else {
        debug!("recv completion for unknown wr_id={wr_id}");
        return;
    };

    // Handle immediate-data control messages (credit ACKs and CLOSE).
    //
    // Important: we keep the receive depth stable by reposting the *same* buffer for control
    // messages (no payload), avoiding allocation/registration in this hot path.
    if c.opcode == WorkCompletionOperationType::ReceiveWithImmediate {
        if let Some(imm) = c.imm_data {
            if is_close_imm(imm) {
                debug!("received CLOSE message from peer");
                shutdown_received.store(true, Ordering::SeqCst);
                repost_buffer_with_retry(qp, posted_recvs, posted, next_wr_id, context, pool, recv_len).await;
                return;
            }

            if let Some(credits) = decode_credit_imm(imm) {
                if credits > 0 {
                    flow_controller.release(credits as usize);
                    debug!("received credit ACK: {} credits", credits);
                }
                repost_buffer_with_retry(qp, posted_recvs, posted, next_wr_id, context, pool, recv_len).await;
                return;
            }
        } else {
            warn!(
                "ReceiveWithImmediate completion without imm_data (wr_id={})",
                wr_id
            );
        }
    }

    // Process completion based on status
    if c.status != WorkCompletionStatus::Success {
        warn!(
            "RDMA recv failed: status={:?} opcode={:?} wr_id={}",
            c.status, c.opcode, wr_id
        );
    } else if c.opcode != WorkCompletionOperationType::Receive
        && c.opcode != WorkCompletionOperationType::ReceiveWithImmediate
    {
        warn!(
            "unexpected recv completion opcode={:?} wr_id={}",
            c.opcode, wr_id
        );
    } else {
        // Successfully received data - extract message
        extract_received_message(posted, c.byte_len as usize, recv_len, recv_manager);
    }

    // Repost receive buffer (never permanently shrink RQ; retry with backoff on allocation failure).
    repost_new_buffer_with_retry(qp, posted_recvs, next_wr_id, context, pool, recv_len).await;

    // Best-effort: top-up RQ back to the desired depth if we started under-filled.
    top_up_receive_queue(qp, posted_recvs, next_wr_id, context, pool, recv_len, recv_manager.target_depth).await;

    // Send credit ACK if threshold reached
    if c.status == WorkCompletionStatus::Success {
        maybe_send_credit_ack(recv_not_acked, buf_ack_batch, qp, next_wr_id).await;
    }
}

/// Repost an existing buffer with retry logic.
async fn repost_buffer_with_retry(
    qp: &Arc<Mutex<GenericQueuePair>>,
    posted_recvs: &Arc<DashMap<u64, PostedRecvBuf>>,
    posted: PostedRecvBuf,
    next_wr_id: &Arc<AtomicU64>,
    context: &Arc<RdmaContext>,
    pool: &Arc<RdmaBufferPool>,
    recv_len: usize,
) {
    let new_wr_id = next_wr_id.fetch_add(1, Ordering::Relaxed);
    if let Err(e) = post_recv_wr(qp, posted_recvs, new_wr_id, posted, recv_len).await {
        warn!("failed to repost recv buffer (will retry with fresh buffer): {e}");
        repost_new_buffer_with_retry(qp, posted_recvs, next_wr_id, context, pool, recv_len).await;
    }
}

/// Allocate and post a new receive buffer with retry logic.
async fn repost_new_buffer_with_retry(
    qp: &Arc<Mutex<GenericQueuePair>>,
    posted_recvs: &Arc<DashMap<u64, PostedRecvBuf>>,
    next_wr_id: &Arc<AtomicU64>,
    context: &Arc<RdmaContext>,
    pool: &Arc<RdmaBufferPool>,
    recv_len: usize,
) {
    let mut backoff_ms: u64 = 0;
    loop {
        let new_wr_id = next_wr_id.fetch_add(1, Ordering::Relaxed);
        match alloc_recv_buffer(context, pool, recv_len) {
            Ok(buf) => match post_recv_wr(qp, posted_recvs, new_wr_id, buf, recv_len).await {
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
        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
    }
}

/// Top up the receive queue to the target depth.
async fn top_up_receive_queue(
    qp: &Arc<Mutex<GenericQueuePair>>,
    posted_recvs: &Arc<DashMap<u64, PostedRecvBuf>>,
    next_wr_id: &Arc<AtomicU64>,
    context: &Arc<RdmaContext>,
    pool: &Arc<RdmaBufferPool>,
    recv_len: usize,
    target_depth: usize,
) {
    let mut extra_posts: usize = 0;
    while posted_recvs.len() < target_depth && extra_posts < 16 {
        let wr_id = next_wr_id.fetch_add(1, Ordering::Relaxed);
        let buf = match alloc_recv_buffer(context, pool, recv_len) {
            Ok(b) => b,
            Err(e) => {
                warn!("failed to allocate recv buffer for depth top-up: {e}");
                break;
            }
        };
        match post_recv_wr(qp, posted_recvs, wr_id, buf, recv_len).await {
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
    recv_manager: &Arc<RecvBufferManager>,
) {
    if received < 4 {
        warn!("RDMA recv too short: {} bytes", received);
        return;
    }

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
                warn!(
                    "RDMA recv bad framing: msg_len={} received={}",
                    msg_len, received
                );
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
    recv_not_acked: &Arc<AtomicU64>,
    buf_ack_batch: usize,
    qp: &Arc<Mutex<GenericQueuePair>>,
    next_wr_id: &Arc<AtomicU64>,
) {
    let acked = recv_not_acked.fetch_add(1, Ordering::Relaxed) + 1;
    if acked as usize >= buf_ack_batch {
        let credits_to_ack = recv_not_acked.swap(0, Ordering::Relaxed) as u32;
        if credits_to_ack > 0 {
            let mut remaining = credits_to_ack;
            while remaining > 0 {
                let chunk = remaining.min(u16::MAX as u32);
                if let Err(e) = send_credit_ack(qp, next_wr_id, chunk).await {
                    warn!("failed to post credit ACK: {}", e);
                    // Return the remaining credits (including this chunk) to counter so we retry next time.
                    recv_not_acked.fetch_add(remaining as u64, Ordering::Relaxed);
                    break;
                }
                remaining -= chunk;
            }
        }
    }
}
