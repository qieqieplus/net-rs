//! Internal types used by the RDMA transport.

use crate::drivers::rdma::buffer::RdmaMr;
use crate::drivers::rdma::slab_allocator::SlabChunk;

/// Buffer types for keeping send data alive until completion.
#[allow(dead_code)]
pub(crate) enum SendKeepalive {
    Slab(SlabChunk),
    Dynamic(RdmaMr),
}

/// Tracks an unsignaled send until a signaled completion confirms it's safe to release.
pub(crate) struct PendingSend {
    pub wr_id: u64,
    /// Stored only to keep the underlying registered memory alive until it's safe to release.
    pub _keepalive: SendKeepalive,
}

/// Buffer types for posted receive work requests.
pub(crate) enum PostedRecvBuf {
    Slab(SlabChunk),
    Dynamic(RdmaMr),
}

impl PostedRecvBuf {
    /// Get the scatter-gather entry parameters for this buffer.
    pub fn sge(&self, recv_len: usize) -> (u32, u64, u32) {
        match self {
            PostedRecvBuf::Slab(chunk) => {
                // Defensive: cap at actual buffer capacity
                let len = recv_len.min(chunk.capacity()) as u32;
                (chunk.lkey(), chunk.as_ptr() as u64, len)
            }
            PostedRecvBuf::Dynamic(mr) => {
                // Defensive: cap at actual buffer length
                let len = recv_len.min(mr.as_slice().len()) as u32;
                (mr.lkey(), mr.as_slice().as_ptr() as u64, len)
            }
        }
    }
}

/// Wrapper around a slab chunk that implements AsRef<[u8]> for use with Bytes::from_owner.
pub(crate) struct SlabPayloadOwner {
    pub chunk: SlabChunk,
    pub offset: usize,
    pub len: usize,
}

impl AsRef<[u8]> for SlabPayloadOwner {
    fn as_ref(&self) -> &[u8] {
        &self.chunk.as_slice()[self.offset..self.offset + self.len]
    }
}
