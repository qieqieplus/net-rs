//! Send path helpers and supporting types.

use super::protocol::{encode_credit_imm, MSG_HEADER_SIZE};
use super::types::SendKeepalive;
use crate::drivers::rdma::buffer::RdmaMr;
use crate::drivers::rdma::context::RdmaContext;
use bytes::{Bytes, BytesMut};
use sideway::ibverbs::queue_pair::{GenericQueuePair, QueuePair, PostSendGuard as _, WorkRequestFlags};
use std::io;
use std::sync::Arc;
use std::cell::{Cell, RefCell};
use tracing::{debug, warn};

/// Send a credit ACK to restore sender's credits.
pub(crate) async fn send_credit_ack(
    qp: &RefCell<GenericQueuePair>,
    next_wr_id: &Cell<u64>,
    credits: u32,
) -> io::Result<()> {
    let ack_wr_id = {
        let id = next_wr_id.get();
        next_wr_id.set(id.wrapping_add(1));
        id
    };

    // Synchronous borrow (safe as we don't await while holding it, except for the post which is sync)
    {
        let mut qp_guard = qp.borrow_mut();
        let mut guard = qp_guard.start_post_send();
        let wr = guard.construct_wr(ack_wr_id, WorkRequestFlags::Signaled);
        wr.setup_send_imm(encode_credit_imm(credits));
        guard.post().map_err(|e| io::Error::other(e.to_string()))?;
    }

    debug!("sent credit ACK: {} credits", credits);
    Ok(())
}

/// Prepare send buffer, preferring slab allocation over dynamic registration.
pub(crate) fn prepare_send_buffer(
    context: &Arc<RdmaContext>,
    buf: &Bytes,
    total_len: usize,
) -> io::Result<(u32, u64, u32, SendKeepalive)> {
    if let Some(mut chunk) = context.slab.alloc(total_len) {
        let slice = chunk.as_mut_slice();
        slice[0..MSG_HEADER_SIZE].copy_from_slice(&(buf.len() as u32).to_be_bytes());
        slice[MSG_HEADER_SIZE..MSG_HEADER_SIZE + buf.len()].copy_from_slice(buf.as_ref());
        Ok((
            chunk.lkey(),
            chunk.as_ptr() as u64,
            total_len as u32,
            SendKeepalive::Slab(chunk),
        ))
    } else {
        // Slow path: slab exhausted, falling back to dynamic registration.
        // This is expensive (syscall + TLB flush) and should be monitored.
        warn!(
            total_len,
            "slab allocator exhausted, falling back to slow dynamic memory registration"
        );
        let mut framed = BytesMut::with_capacity(total_len);
        framed.extend_from_slice(&(buf.len() as u32).to_be_bytes());
        framed.extend_from_slice(buf.as_ref());
        let mut mr = RdmaMr::register(context, framed)
            .ok_or_else(|| io::Error::other("failed to register memory"))?;
        let lkey = mr.lkey();
        let addr = mr.as_mut_slice().as_ptr() as u64;
        let len = mr.as_slice().len() as u32;
        Ok((lkey, addr, len, SendKeepalive::Dynamic(mr)))
    }
}
