//! Transport configuration for RDMA flow control and batching.

/// Configuration for RDMA transport flow control and batching.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Number of receives before sending credit ACK to peer.
    pub buf_ack_batch: usize,
    /// Number of sends before requesting completion signal.
    pub send_signal_batch: usize,
    /// Maximum outstanding sends before blocking on credits.
    pub max_outstanding_sends: usize,
    /// Maximum outstanding RDMA work requests.
    pub max_rdma_wr: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            buf_ack_batch: 8,
            send_signal_batch: 8,
            max_outstanding_sends: 32,
            max_rdma_wr: 128,
        }
    }
}

impl TransportConfig {
    /// Create a new configuration with default values.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the buffer ACK batch size.
    #[inline]
    pub fn with_buf_ack_batch(mut self, n: usize) -> Self {
        self.buf_ack_batch = n;
        self
    }

    /// Set the send signal batch size.
    #[inline]
    pub fn with_send_signal_batch(mut self, n: usize) -> Self {
        self.send_signal_batch = n;
        self
    }

    /// Set the maximum outstanding sends.
    #[inline]
    pub fn with_max_outstanding_sends(mut self, n: usize) -> Self {
        self.max_outstanding_sends = n;
        self
    }

    /// Set the maximum RDMA work requests.
    #[inline]
    pub fn with_max_rdma_wr(mut self, n: usize) -> Self {
        self.max_rdma_wr = n;
        self
    }
}
