//! Completion future implementations for RDMA operations.

use crate::drivers::rdma::poller::{Completion, RdmaPoller};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Future that waits for an RDMA completion event.
///
/// Tracks the completion state and cancels waker registration on drop
/// if the completion hasn't arrived yet.
pub struct CompletionFuture {
    poller: Arc<RdmaPoller>,
    wr_id: u64,
    completed: bool,
}

impl CompletionFuture {
    /// Create a new completion future for the given work request ID.
    #[inline]
    pub fn new(poller: Arc<RdmaPoller>, wr_id: u64) -> Self {
        Self {
            poller,
            wr_id,
            completed: false,
        }
    }
}

impl Future for CompletionFuture {
    type Output = io::Result<Completion>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(c) = this.poller.take_completion(this.wr_id) {
            this.completed = true;
            return Poll::Ready(Ok(c));
        }
        this.poller.register(this.wr_id, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for CompletionFuture {
    fn drop(&mut self) {
        // If dropped before completion arrives (timeout/cancellation),
        // cancel waker registration to avoid leaking resources.
        if !self.completed {
            self.poller.cancel(self.wr_id);
        }
    }
}

/// Creates a future that waits for a completion with an owned reference to the poller.
///
/// This is the standard way to wait for RDMA completions. The future owns an `Arc<RdmaPoller>`
/// so it can outlive temporary references to the transport.
#[inline]
pub fn wait_for_completion(poller: Arc<RdmaPoller>, wr_id: u64) -> CompletionFuture {
    CompletionFuture::new(poller, wr_id)
}

/// Alias for backward compatibility.
#[inline]
pub fn wait_for_completion_owned(
    poller: Arc<RdmaPoller>,
    wr_id: u64,
) -> impl Future<Output = io::Result<Completion>> {
    wait_for_completion(poller, wr_id)
}

/// Creates a completion future (alias for `wait_for_completion`).
///
/// Kept for API compatibility with existing code.
#[inline]
pub fn make_completion_future(
    poller: Arc<RdmaPoller>,
    wr_id: u64,
) -> impl Future<Output = io::Result<Completion>> {
    wait_for_completion(poller, wr_id)
}
