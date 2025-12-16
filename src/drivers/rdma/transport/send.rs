//! Send path helpers and supporting types.

use super::completion::wait_for_completion_owned;
use super::protocol::encode_credit_imm;
use super::types::PendingSend;
use crate::drivers::rdma::poller::RdmaPoller;
use sideway::ibverbs::completion::WorkCompletionStatus;
use sideway::ibverbs::queue_pair::{GenericQueuePair, QueuePair, PostSendGuard as _, WorkRequestFlags};
use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tracing::debug;

/// Guard for a posted send operation that provides completion waiting.
pub(crate) struct PostSendGuard<'a> {
    pub poller: &'a Arc<RdmaPoller>,
    pub wr_id: u64,
}

impl<'a> PostSendGuard<'a> {
    pub async fn wait(self) -> io::Result<()> {
        // Poll for completion
        let c = wait_for_completion_owned(Arc::clone(self.poller), self.wr_id).await?;
        if c.status != WorkCompletionStatus::Success {
            return Err(io::Error::other(format!(
                "RDMA send failed: status={:?} opcode={:?}",
                c.status, c.opcode
            )));
        }
        Ok(())
    }
}

/// Ensures a signaled send's keepalive is eventually released even if the caller cancels the send future.
pub(crate) struct SignaledSendReaper {
    pub poller: Arc<RdmaPoller>,
    pub pending_sends: Arc<StdMutex<VecDeque<PendingSend>>>,
    pub wr_id: u64,
    pub done: bool,
}

impl Drop for SignaledSendReaper {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        // Best-effort: if we are running inside a Tokio runtime, detach a task that waits for the
        // completion and cleans pending sends up to (and including) this wr_id.
        let Ok(handle) = Handle::try_current() else {
            // No runtime available; keepalive remains in pending_sends until the transport is dropped.
            return;
        };

        let poller = Arc::clone(&self.poller);
        let pending_sends = Arc::clone(&self.pending_sends);
        let wr_id = self.wr_id;

        handle.spawn(async move {
            // Wait for completion (or cancellation of the runtime); on completion, clean up.
            let _ = wait_for_completion_owned(poller, wr_id).await;
            let mut pending = pending_sends.lock().expect("pending_sends poisoned");
            while let Some(front) = pending.front() {
                if front.wr_id <= wr_id {
                    pending.pop_front();
                } else {
                    break;
                }
            }
        });
    }
}

/// Send a credit ACK to restore sender's credits.
pub(crate) async fn send_credit_ack(
    qp: &Arc<Mutex<GenericQueuePair>>,
    next_wr_id: &Arc<AtomicU64>,
    credits: u32,
) -> io::Result<()> {
    let ack_wr_id = next_wr_id.fetch_add(1, Ordering::Relaxed);

    let mut qp_guard = qp.lock().await;
    let mut guard = qp_guard.start_post_send();
    let wr = guard.construct_wr(ack_wr_id, WorkRequestFlags::Signaled);
    wr.setup_send_imm(encode_credit_imm(credits));
    guard.post().map_err(|e| io::Error::other(e.to_string()))?;

    debug!("sent credit ACK: {} credits", credits);
    Ok(())
}
