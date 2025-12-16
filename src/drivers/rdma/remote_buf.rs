/// Remote buffer descriptor for RDMA operations.
///
/// Holds the remote virtual address, length, and remote key (rkey) needed
/// to perform one-sided RDMA READ/WRITE operations. This is the Rust
/// equivalent of `RDMARemoteBuf` from the C++ reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteBuf {
    addr: u64,
    length: u64,
    rkey: u32,
}

impl RemoteBuf {
    /// Create a new remote buffer descriptor.
    pub fn new(addr: u64, length: u64, rkey: u32) -> Self {
        Self { addr, length, rkey }
    }

    /// Create an invalid (empty) remote buffer.
    pub fn empty() -> Self {
        Self {
            addr: 0,
            length: 0,
            rkey: 0,
        }
    }

    /// Get the remote virtual address.
    pub fn addr(&self) -> u64 {
        self.addr
    }

    /// Get the buffer length.
    pub fn len(&self) -> u64 {
        self.length
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Get the remote key.
    pub fn rkey(&self) -> u32 {
        self.rkey
    }

    /// Check if this is a valid remote buffer.
    pub fn is_valid(&self) -> bool {
        self.addr != 0 && self.length > 0
    }

    /// Advance the buffer by `n` bytes, returning true on success.
    ///
    /// Returns false if `n` exceeds the current length.
    pub fn advance(&mut self, n: u64) -> bool {
        if n > self.length {
            return false;
        }
        self.addr += n;
        self.length -= n;
        true
    }

    /// Reduce the buffer length by `n` bytes from the end, returning true on success.
    ///
    /// Returns false if `n` exceeds the current length.
    pub fn subtract(&mut self, n: u64) -> bool {
        if n > self.length {
            return false;
        }
        self.length -= n;
        true
    }

    /// Create a subrange of this buffer.
    ///
    /// Returns an empty buffer if the range is invalid.
    pub fn subrange(&self, offset: u64, len: u64) -> RemoteBuf {
        if offset + len > self.length {
            return RemoteBuf::empty();
        }
        RemoteBuf {
            addr: self.addr + offset,
            length: len,
            rkey: self.rkey,
        }
    }

    /// Get the first `len` bytes of this buffer.
    pub fn first(&self, len: u64) -> RemoteBuf {
        self.subrange(0, len)
    }

    /// Take the first `len` bytes, advancing this buffer.
    pub fn take_first(&mut self, len: u64) -> RemoteBuf {
        let buf = self.first(len);
        self.advance(len);
        buf
    }

    /// Get the last `len` bytes of this buffer.
    pub fn last(&self, len: u64) -> RemoteBuf {
        if len > self.length {
            return RemoteBuf::empty();
        }
        self.subrange(self.length - len, len)
    }

    /// Take the last `len` bytes, reducing this buffer's length.
    pub fn take_last(&mut self, len: u64) -> RemoteBuf {
        let buf = self.last(len);
        self.subtract(len);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remote_buf_basic() {
        let buf = RemoteBuf::new(0x1000, 256, 0xdeadbeef);
        assert_eq!(buf.addr(), 0x1000);
        assert_eq!(buf.len(), 256);
        assert_eq!(buf.rkey(), 0xdeadbeef);
        assert!(buf.is_valid());
        assert!(!buf.is_empty());

        let empty = RemoteBuf::empty();
        assert!(!empty.is_valid());
        assert!(empty.is_empty());
    }

    #[test]
    fn test_remote_buf_advance() {
        let mut buf = RemoteBuf::new(0x1000, 256, 0x123);

        assert!(buf.advance(100));
        assert_eq!(buf.addr(), 0x1064);
        assert_eq!(buf.len(), 156);

        assert!(!buf.advance(200));
        assert_eq!(buf.addr(), 0x1064);
        assert_eq!(buf.len(), 156);
    }

    #[test]
    fn test_remote_buf_subtract() {
        let mut buf = RemoteBuf::new(0x1000, 256, 0x123);

        assert!(buf.subtract(100));
        assert_eq!(buf.addr(), 0x1000);
        assert_eq!(buf.len(), 156);

        assert!(!buf.subtract(200));
        assert_eq!(buf.len(), 156);
    }

    #[test]
    fn test_remote_buf_subrange() {
        let buf = RemoteBuf::new(0x1000, 256, 0x123);

        let sub = buf.subrange(64, 128);
        assert_eq!(sub.addr(), 0x1040);
        assert_eq!(sub.len(), 128);
        assert_eq!(sub.rkey(), 0x123);

        let invalid = buf.subrange(200, 100);
        assert!(!invalid.is_valid());
    }

    #[test]
    fn test_remote_buf_first_last() {
        let buf = RemoteBuf::new(0x1000, 256, 0x123);

        let first = buf.first(64);
        assert_eq!(first.addr(), 0x1000);
        assert_eq!(first.len(), 64);

        let last = buf.last(64);
        assert_eq!(last.addr(), 0x10c0);
        assert_eq!(last.len(), 64);
    }

    #[test]
    fn test_remote_buf_take_first() {
        let mut buf = RemoteBuf::new(0x1000, 256, 0x123);

        let first = buf.take_first(64);
        assert_eq!(first.addr(), 0x1000);
        assert_eq!(first.len(), 64);
        assert_eq!(buf.addr(), 0x1040);
        assert_eq!(buf.len(), 192);
    }

    #[test]
    fn test_remote_buf_take_last() {
        let mut buf = RemoteBuf::new(0x1000, 256, 0x123);

        let last = buf.take_last(64);
        assert_eq!(last.addr(), 0x10c0);
        assert_eq!(last.len(), 64);
        assert_eq!(buf.addr(), 0x1000);
        assert_eq!(buf.len(), 192);
    }
}
