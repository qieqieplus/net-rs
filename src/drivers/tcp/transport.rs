use crate::transport::{BufferPool, Transport};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut, Buf};
use std::cell::RefCell;
use std::io;
use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt, AsyncWriteRent, Splitable};
use monoio::net::TcpStream;
use monoio::net::tcp::{TcpOwnedReadHalf, TcpOwnedWriteHalf};

const MAX_MSG_SIZE: usize = 512 * 1024 * 1024; // 512MB

#[derive(Clone, Default)]
pub struct TcpBufferPool;

impl BufferPool for TcpBufferPool {
    fn alloc(&self, size: usize) -> BytesMut {
        BytesMut::with_capacity(size)
    }
}

pub struct TcpTransport {
    read: RefCell<TcpOwnedReadHalf>,
    write: RefCell<TcpOwnedWriteHalf>,
    pool: TcpBufferPool,
}

impl TcpTransport {
    pub fn new(stream: TcpStream) -> Self {
        let (read, write) = stream.into_split();
        Self {
            read: RefCell::new(read),
            write: RefCell::new(write),
            pool: TcpBufferPool,
        }
    }
}

#[async_trait(?Send)]
impl Transport for TcpTransport {
    // SAFETY: This is safe in monoio's thread-per-core model because:
    // 1. TcpTransport is !Send (due to RefCell), so it can only be used on one thread
    // 2. monoio is a single-threaded executor, so only one task runs at a time
    // 3. The ?Send bound on async_trait ensures the future is !Send
    // Therefore, no concurrent access to the RefCell is possible.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn send(&self, buf: Bytes) -> io::Result<()> {
        let len = buf.len();
        if len > MAX_MSG_SIZE {
             return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("message too large: {} > {}", len, MAX_MSG_SIZE)
            ));
        }

        // We need to write 4-byte length + body.
        let mut header = Vec::with_capacity(4);
        header.extend_from_slice(&(len as u32).to_be_bytes());
        
        let mut write = self.write.borrow_mut();
        
        // Write length
        let (res, _) = write.write_all(header).await;
        res?;
        
        // Write body
        let (res, _) = write.write_all(buf).await;
        res?;
        
        write.flush().await?;
        
        Ok(())
    }

    // SAFETY: Same reasoning as send() above.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn recv(&self) -> io::Result<Bytes> {
        let mut read = self.read.borrow_mut();
        
        // Read 4-byte length
        let header_buf = BytesMut::with_capacity(4);
        let (res, mut header_buf): (std::io::Result<usize>, BytesMut) = read.read_exact(header_buf).await;
        
        // Handle error or EOF
        let n = res?;
        if n != 4 {
             return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to read message length"));
        }
        
        let len = header_buf.get_u32() as usize;
        
        if len > MAX_MSG_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("message too large: {} > {}", len, MAX_MSG_SIZE)
            ));
        }
        
        let buf = BytesMut::with_capacity(len);
        let (res, buf): (std::io::Result<usize>, BytesMut) = read.read_exact(buf).await;
        let n = res?;
        if n != len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to read message body"));
        }
        
        Ok(buf.freeze())
    }

    fn alloc_buf(&self, size: usize) -> BytesMut {
        self.pool.alloc(size)
    }
}
