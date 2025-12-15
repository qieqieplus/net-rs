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
