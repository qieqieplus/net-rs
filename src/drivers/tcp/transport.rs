use crate::transport::{framing, BufferPool, Transport};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Clone, Default)]
pub struct TcpBufferPool;

impl BufferPool for TcpBufferPool {
    fn alloc(&self, size: usize) -> BytesMut {
        BytesMut::with_capacity(size)
    }
}

pub struct TcpTransport {
    read: Mutex<tokio::net::tcp::OwnedReadHalf>,
    write: Mutex<tokio::net::tcp::OwnedWriteHalf>,
    pool: TcpBufferPool,
    max_message_size: usize,
}

impl TcpTransport {
    pub fn new(stream: TcpStream) -> Self {
        let (read, write) = stream.into_split();
        Self {
            read: Mutex::new(read),
            write: Mutex::new(write),
            pool: TcpBufferPool,
            max_message_size: framing::DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn send(&self, buf: Bytes) -> io::Result<()> {
        let mut write = self.write.lock().await;
        // 4-byte generic length framing for "Message" semantics
        // to match the expectation that recv() returns a "Message".
        
        let len = buf.len() as u32;
        write.write_u32(len).await?;
        write.write_all(&buf).await?;
        write.flush().await?;
        Ok(())
    }

    async fn recv(&self) -> io::Result<Bytes> {
        let mut read = self.read.lock().await;
        
        // Read 4-byte length
        let len = read.read_u32().await? as usize;
        
        framing::validate_frame_len(len, self.max_message_size)?;
        
        let mut buf = BytesMut::with_capacity(len);
        // Resize to target length so read_exact usually fills it
        buf.resize(len, 0);
        
        read.read_exact(&mut buf).await?;
        Ok(buf.freeze())
    }

    fn alloc_buf(&self, size: usize) -> BytesMut {
        self.pool.alloc(size)
    }
}
