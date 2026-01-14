use bytes::Bytes;
use flume::{unbounded, Receiver, Sender};

/// Async receive queue fed by the RDMA poller path.
///
/// This decouples completion processing (which needs to repost receives quickly)
/// from application-level `recv()` calls.
pub struct RecvBufferManager {
    tx: Sender<Bytes>,
    rx: Receiver<Bytes>,
    pub target_depth: usize,
}

impl RecvBufferManager {
    pub fn new(target_depth: usize) -> Self {
        let (tx, rx) = unbounded();
        Self {
            tx,
            rx,
            target_depth,
        }
    }

    pub fn push(&self, msg: Bytes) {
        // Best-effort: if receiver is dropped the transport is shutting down.
        let _ = self.tx.send(msg);
    }

    pub async fn recv(&self) -> Option<Bytes> {
        self.rx.recv_async().await.ok()
    }

    #[allow(dead_code)]
    pub fn sender(&self) -> Sender<Bytes> {
        self.tx.clone()
    }
}


