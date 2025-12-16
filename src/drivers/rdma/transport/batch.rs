//! Batched RDMA operations for improved throughput.
//!
//! This module provides batched RDMA READ/WRITE operations that post multiple
//! work requests in a single syscall. Following the reference library's approach:
//!
//! - Multiple WRs are linked and posted via single `ibv_post_send`
//! - Only the last WR in the batch is signaled (reduces CQ overhead)
//! - Partial post failures transition QP to Error state (no way to track)
//!
//! # Note on READ vs WRITE batching
//!
//! RDMA WRITE and READ have different mutability requirements:
//! - **WRITE**: NIC reads from local buffer → `&RdmaMr` (immutable)
//! - **READ**: NIC writes into local buffer → `&mut RdmaMr` (mutable)
//!
//! This module provides type-safe APIs that enforce these requirements:
//! - `rdma_write_batch` - takes `&RdmaMr` (immutable, safe)
//! - `rdma_read_batch` - takes `&mut RdmaMr` (mutable, safe)

use super::RdmaTransport;
use crate::drivers::rdma::buffer::RdmaMr;
use crate::drivers::rdma::RemoteBuf;
use sideway::ibverbs::completion::WorkCompletionStatus;
use sideway::ibverbs::queue_pair::{
    PostSendGuard as _, QueuePair, QueuePairAttribute, QueuePairState, SetScatterGatherEntry,
    WorkRequestFlags,
};
use std::io;
use tracing::warn;

/// A single RDMA WRITE operation in a batch.
///
/// Uses immutable `&RdmaMr` because the NIC only reads from the local buffer.
pub struct RdmaWriteOp<'a> {
    /// Remote buffer descriptor (destination)
    pub remote: &'a RemoteBuf,
    /// Local memory region (source, read by NIC)
    pub local: &'a RdmaMr,
}

/// A single RDMA READ operation in a batch.
///
/// Uses mutable `&mut RdmaMr` because the NIC writes into the local buffer.
pub struct RdmaReadOp<'a> {
    /// Remote buffer descriptor (source)
    pub remote: &'a RemoteBuf,
    /// Local memory region (destination, written by NIC)
    pub local: &'a mut RdmaMr,
}

/// Result of a batched RDMA operation.
#[derive(Debug)]
pub struct BatchResult {
    /// Number of operations that were posted.
    pub posted: usize,
    /// Total bytes transferred (sum of all operations).
    pub total_bytes: u64,
}

impl RdmaTransport {
    /// Execute multiple RDMA WRITE operations in a single batched post.
    ///
    /// This is more efficient than calling `rdma_write_mr` individually because:
    /// 1. Only a single `ibv_post_send` syscall is made
    /// 2. Only the final WR is signaled (reduces completion processing)
    /// 3. Uses RDMA's inherent ordering guarantees
    ///
    /// # Arguments
    /// * `ops` - Slice of RDMA WRITE operations to execute
    ///
    /// # Returns
    /// * `Ok(BatchResult)` on success with statistics
    /// * `Err` if any operation fails (partial failures transition QP to Error)
    ///
    /// # Example
    /// ```ignore
    /// let ops = vec![
    ///     RdmaWriteOp { remote: &remote1, local: &local1 },
    ///     RdmaWriteOp { remote: &remote2, local: &local2 },
    /// ];
    /// transport.rdma_write_batch(&ops).await?;
    /// ```
    pub async fn rdma_write_batch(&self, ops: &[RdmaWriteOp<'_>]) -> io::Result<BatchResult> {
        if ops.is_empty() {
            return Ok(BatchResult {
                posted: 0,
                total_bytes: 0,
            });
        }

        // Validate all operations first
        for (i, op) in ops.iter().enumerate() {
            Self::validate_op(i, op.remote, op.local.as_slice().len(), "WRITE")?;
        }

        // Acquire permits for all operations
        let _permit = self
            .rdma_semaphore
            .acquire_many(ops.len() as u32)
            .await
            .map_err(|_| io::Error::other("RDMA semaphore closed"))?;

        let final_wr_id = self.alloc_wr_id();
        let mut total_bytes: u64 = 0;

        {
            let mut qp_guard = self.qp.lock().await;
            let mut guard = qp_guard.start_post_send();

            for (i, op) in ops.iter().enumerate() {
                let is_last = i == ops.len() - 1;
                let flags = if is_last {
                    WorkRequestFlags::Signaled
                } else {
                    WorkRequestFlags::none()
                };
                let wr_id = if is_last { final_wr_id } else { 0 };

                let wr = guard.construct_wr(wr_id, flags);
                let lkey = op.local.lkey();
                let local_addr = op.local.as_slice().as_ptr() as u64;
                let len = op.local.as_slice().len() as u32;

                unsafe {
                    wr.setup_write(op.remote.rkey(), op.remote.addr())
                        .setup_sge(lkey, local_addr, len);
                }

                total_bytes += len as u64;
            }

            if let Err(e) = guard.post() {
                warn!("RDMA write batch post failed, transitioning QP to Error: {}", e);
                self.force_qp_error_internal(&mut qp_guard);
                return Err(io::Error::other(format!("RDMA write batch post failed: {}", e)));
            }
        }

        let completion = self.wait_for_completion(final_wr_id).await?;
        if completion.status != WorkCompletionStatus::Success {
            return Err(io::Error::other(format!(
                "RDMA write batch failed: status={:?}",
                completion.status
            )));
        }

        Ok(BatchResult {
            posted: ops.len(),
            total_bytes,
        })
    }

    /// Execute multiple RDMA READ operations in a single batched post.
    ///
    /// This is more efficient than calling `rdma_read_mr` individually because:
    /// 1. Only a single `ibv_post_send` syscall is made
    /// 2. Only the final WR is signaled (reduces completion processing)
    /// 3. Uses RDMA's inherent ordering guarantees
    ///
    /// # Arguments
    /// * `ops` - Slice of RDMA READ operations to execute (mutable locals)
    ///
    /// # Returns
    /// * `Ok(BatchResult)` on success with statistics
    /// * `Err` if any operation fails (partial failures transition QP to Error)
    ///
    /// # Example
    /// ```ignore
    /// let ops = vec![
    ///     RdmaReadOp { remote: &remote1, local: &mut local1 },
    ///     RdmaReadOp { remote: &remote2, local: &mut local2 },
    /// ];
    /// transport.rdma_read_batch(&mut ops).await?;
    /// ```
    pub async fn rdma_read_batch(&self, ops: &mut [RdmaReadOp<'_>]) -> io::Result<BatchResult> {
        if ops.is_empty() {
            return Ok(BatchResult {
                posted: 0,
                total_bytes: 0,
            });
        }

        // Validate all operations first
        for (i, op) in ops.iter().enumerate() {
            Self::validate_op(i, op.remote, op.local.as_slice().len(), "READ")?;
        }

        // Acquire permits for all operations
        let _permit = self
            .rdma_semaphore
            .acquire_many(ops.len() as u32)
            .await
            .map_err(|_| io::Error::other("RDMA semaphore closed"))?;

        let final_wr_id = self.alloc_wr_id();
        let mut total_bytes: u64 = 0;

        {
            let mut qp_guard = self.qp.lock().await;
            let mut guard = qp_guard.start_post_send();
            let num_ops = ops.len();

            for (i, op) in ops.iter_mut().enumerate() {
                let is_last = i == num_ops - 1;
                let flags = if is_last {
                    WorkRequestFlags::Signaled
                } else {
                    WorkRequestFlags::none()
                };
                let wr_id = if is_last { final_wr_id } else { 0 };

                let wr = guard.construct_wr(wr_id, flags);
                let lkey = op.local.lkey();
                // Use as_mut_slice to get mutable pointer for DMA write destination
                let local_addr = op.local.as_mut_slice().as_ptr() as u64;
                let len = op.local.as_slice().len() as u32;

                unsafe {
                    wr.setup_read(op.remote.rkey(), op.remote.addr())
                        .setup_sge(lkey, local_addr, len);
                }

                total_bytes += len as u64;
            }

            if let Err(e) = guard.post() {
                warn!("RDMA read batch post failed, transitioning QP to Error: {}", e);
                self.force_qp_error_internal(&mut qp_guard);
                return Err(io::Error::other(format!("RDMA read batch post failed: {}", e)));
            }
        }

        let completion = self.wait_for_completion(final_wr_id).await?;
        if completion.status != WorkCompletionStatus::Success {
            return Err(io::Error::other(format!(
                "RDMA read batch failed: status={:?}",
                completion.status
            )));
        }

        Ok(BatchResult {
            posted: ops.len(),
            total_bytes,
        })
    }

    /// Validate a single RDMA operation's buffers.
    fn validate_op(index: usize, remote: &RemoteBuf, local_len: usize, op_name: &str) -> io::Result<()> {
        if !remote.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("RDMA {} batch op[{}]: invalid remote buffer", op_name, index),
            ));
        }
        if local_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("RDMA {} batch op[{}]: empty local buffer", op_name, index),
            ));
        }
        if local_len > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "RDMA {} batch op[{}]: local buffer length ({}) exceeds u32::MAX ({}), cannot post WR",
                    op_name,
                    index,
                    local_len,
                    u32::MAX
                ),
            ));
        }
        if local_len as u64 > remote.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "RDMA {} batch op[{}]: local buffer ({}) exceeds remote buffer ({})",
                    op_name, index, local_len, remote.len()
                ),
            ));
        }
        Ok(())
    }

    /// Internal helper to force QP to Error state (used after partial post failure).
    fn force_qp_error_internal(
        &self,
        qp: &mut sideway::ibverbs::queue_pair::GenericQueuePair,
    ) {
        let current_state = qp.state();
        if matches!(
            current_state,
            QueuePairState::Error | QueuePairState::Reset
        ) {
            return;
        }

        let mut attr = QueuePairAttribute::new();
        attr.setup_state(QueuePairState::Error);

        if let Err(e) = qp.modify(&attr) {
            warn!("failed to transition QP to Error state: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_result_default() {
        let result = BatchResult {
            posted: 0,
            total_bytes: 0,
        };
        assert_eq!(result.posted, 0);
        assert_eq!(result.total_bytes, 0);
    }
}
