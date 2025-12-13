use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::io;

/// Abstract factory for allocating buffers.
/// 
/// In TCP, this just allocates heap memory.
/// In RDMA, this allocates from a registered memory region (MR).
pub trait BufferPool: Send + Sync + 'static {
    fn alloc(&self, size: usize) -> BytesMut;
}

/// A generic transport that can send and receive data.
/// 
/// This abstracts over a TCP Stream or an RDMA Queue Pair.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send a buffer to the peer.
    /// 
    /// The buffer should ideally be allocated via `alloc_buf` to support zero-copy on RDMA.
    async fn send(&self, buf: Bytes) -> io::Result<()>;

    /// Receive a message from the peer.
    /// 
    /// For message-oriented transports (RDMA), this returns a complete message.
    /// For stream-oriented transports (TCP), this returns the next available chunk of data.
    async fn recv(&self) -> io::Result<Bytes>;

    /// Allocate a buffer compatible with this transport.
    fn alloc_buf(&self, size: usize) -> BytesMut;
}
