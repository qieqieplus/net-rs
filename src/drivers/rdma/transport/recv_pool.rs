use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;

/// Async receive queue fed by the RDMA poller path.
///
/// This decouples completion processing (which needs to repost receives quickly)
/// from application-level `recv()` calls.
pub struct RecvBufferManager {
    tx: UnboundedSender<Bytes>,
    rx: Mutex<UnboundedReceiver<Bytes>>,
    pub target_depth: usize,
}

impl RecvBufferManager {
    pub fn new(target_depth: usize) -> Arc<Self> {
        let (tx, rx) = unbounded_channel();
        Arc::new(Self {
            tx,
            rx: Mutex::new(rx),
            target_depth,
        })
    }

    pub fn push(&self, msg: Bytes) {
        // Best-effort: if receiver is dropped the transport is shutting down.
        let _ = self.tx.send(msg);
    }

    pub async fn recv(&self) -> Option<Bytes> {
        self.rx.lock().await.recv().await
    }

    #[allow(dead_code)]
    pub fn sender(&self) -> UnboundedSender<Bytes> {
        self.tx.clone()
    }
}


