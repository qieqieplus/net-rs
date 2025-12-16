use crate::transport::{BufferPool, Transport};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

const MAX_MSG_SIZE: usize = 1024 * 1024; // 1MB

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
}

impl TcpTransport {
    pub fn new(stream: TcpStream) -> Self {
        let (read, write) = stream.into_split();
        Self {
            read: Mutex::new(read),
            write: Mutex::new(write),
            pool: TcpBufferPool,
        }
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn send(&self, buf: Bytes) -> io::Result<()> {
        let mut write = self.write.lock().await;
        
        let len = buf.len();
        if len > MAX_MSG_SIZE {
             return Err(io::Error::other(format!(
                "message too large: {} > {}",
                len, MAX_MSG_SIZE
            )));
        }

        write.write_u32(len as u32).await?;
        write.write_all(&buf).await?;
        write.flush().await?;
        Ok(())
    }

    async fn recv(&self) -> io::Result<Bytes> {
        let mut read = self.read.lock().await;
        
        // Read 4-byte length
        let len = read.read_u32().await? as usize;
        
        if len > MAX_MSG_SIZE {
            return Err(io::Error::other(format!(
                "message too large: {} > {}",
                len, MAX_MSG_SIZE
            )));
        }
        
        let mut buf = BytesMut::with_capacity(len);
        // Safer than unsafe set_len, though slightly slower.
        buf.resize(len, 0);
        
        read.read_exact(&mut buf).await?;
        Ok(buf.freeze())
    }

    fn alloc_buf(&self, size: usize) -> BytesMut {
        self.pool.alloc(size)
    }
}
