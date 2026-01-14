//! Completion future implementations for RDMA operations.
//! 
//! This module provides wrappers around `RdmaPoller::wait()` that return
//! `io::Result<Completion>` for consistency with the rest of the transport API.

use crate::drivers::rdma::poller::{Completion, RdmaPoller};
use sideway::ibverbs::completion::WorkCompletionStatus;
use std::future::Future;
use std::io;
use std::rc::Rc;

/// Wait for a completion and convert it to an io::Result.
/// 
/// Returns `Ok(completion)` on success, or `Err` if the RDMA operation failed.
#[inline]
pub async fn wait_for_completion(poller: Rc<RdmaPoller>, wr_id: u64) -> io::Result<Completion> {
    let completion = poller.wait(wr_id).await;
    if completion.status != WorkCompletionStatus::Success {
        return Err(io::Error::other(format!(
            "RDMA completion error: {:?}",
            completion.status
        )));
    }
    Ok(completion)
}

/// Creates a completion future (alias for `wait_for_completion`).
#[inline]
pub fn make_completion_future(
    poller: Rc<RdmaPoller>,
    wr_id: u64,
) -> impl Future<Output = io::Result<Completion>> {
    wait_for_completion(poller, wr_id)
}

