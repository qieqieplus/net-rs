//! Completion future implementations for RDMA operations.

use crate::drivers::rdma::poller::{Completion, RdmaPoller};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Creates a future that waits for a completion with an owned reference to the poller.
///
/// This is useful when the poller needs to outlive the transport reference,
/// such as in the SignaledSendReaper cleanup task.
pub fn wait_for_completion_owned(
    poller: Arc<RdmaPoller>,
    wr_id: u64,
) -> impl Future<Output = io::Result<Completion>> {
    struct OwnedCompletionFuture {
        poller: Arc<RdmaPoller>,
        wr_id: u64,
        completed: bool,
    }

    impl Future for OwnedCompletionFuture {
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

    impl Drop for OwnedCompletionFuture {
        fn drop(&mut self) {
            if !self.completed {
                self.poller.cancel(self.wr_id);
            }
        }
    }

    OwnedCompletionFuture {
        poller,
        wr_id,
        completed: false,
    }
}

/// Creates a completion future that borrows the poller.
///
/// This is the standard completion wait used by most transport operations.
pub fn make_completion_future(
    poller: Arc<RdmaPoller>,
    wr_id: u64,
) -> impl Future<Output = io::Result<Completion>> {
    struct CompletionFuture {
        poller: Arc<RdmaPoller>,
        wr_id: u64,
        completed: bool,
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
            // If the waiter is dropped (timeout/cancellation) before the completion arrives,
            // avoid leaking wakers/completions inside the poller.
            if !self.completed {
                self.poller.cancel(self.wr_id);
            }
        }
    }

    CompletionFuture {
        poller,
        wr_id,
        completed: false,
    }
}
