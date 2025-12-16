//! RDMA READ/WRITE operations.
//!
//! This module provides one-sided RDMA operations for zero-copy data transfers.

use super::RdmaTransport;
use crate::drivers::rdma::buffer::RdmaMr;
use crate::drivers::rdma::RemoteBuf;
use crate::transport::BufferPool;
use sideway::ibverbs::completion::WorkCompletionStatus;
use sideway::ibverbs::queue_pair::{PostSendGuard as _, QueuePair, SetScatterGatherEntry, WorkRequestFlags};
use std::io;

/// Validate remote and local buffers for an RDMA operation.
///
/// Returns `Ok(())` if buffers are valid, or an appropriate `io::Error` otherwise.
#[inline]
fn validate_rdma_buffers(remote: &RemoteBuf, local_len: usize, op_name: &str) -> io::Result<()> {
    if !remote.is_valid() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{}: invalid remote buffer", op_name),
        ));
    }
    if local_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{}: empty local buffer", op_name),
        ));
    }
    if local_len as u64 > remote.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{}: local buffer ({}) exceeds remote buffer ({})",
                op_name,
                local_len,
                remote.len()
            ),
        ));
    }
    Ok(())
}

impl RdmaTransport {
    /// Perform an RDMA WRITE operation to the remote buffer.
    ///
    /// Writes the local data to the remote memory region using one-sided RDMA.
    /// This is a zero-copy operation that does not involve the remote CPU.
    ///
    /// # Arguments
    /// * `remote` - Remote buffer descriptor (address, length, rkey)
    /// * `local` - Local data to write
    pub async fn rdma_write(&self, remote: &RemoteBuf, local: &[u8]) -> io::Result<()> {
        validate_rdma_buffers(remote, local.len(), "RDMA WRITE")?;

        // Allocate and register local memory for RDMA
        // (fallback path; callers can avoid this by using rdma_write_mr).
        let mut buf = self.pool.alloc(local.len());
        buf.extend_from_slice(local);
        let local_mr = RdmaMr::register(&self.context, buf)
            .ok_or_else(|| io::Error::other("RDMA WRITE: failed to register local memory"))?;

        self.rdma_write_mr(remote, &local_mr).await
    }

    /// Perform an RDMA READ operation from the remote buffer.
    ///
    /// Reads data from the remote memory region into the local buffer using one-sided RDMA.
    /// This is a zero-copy operation that does not involve the remote CPU.
    ///
    /// # Arguments
    /// * `remote` - Remote buffer descriptor (address, length, rkey)
    /// * `local` - Local buffer to read into
    pub async fn rdma_read(&self, remote: &RemoteBuf, local: &mut [u8]) -> io::Result<()> {
        validate_rdma_buffers(remote, local.len(), "RDMA READ")?;

        // Allocate and register a scratch local region for RDMA, then copy back.
        // Callers can avoid this per-op registration by using rdma_read_mr.
        let mut buf = self.pool.alloc(local.len());
        unsafe {
            buf.set_len(local.len());
        }
        let mut local_mr = RdmaMr::register(&self.context, buf)
            .ok_or_else(|| io::Error::other("RDMA READ: failed to register local memory"))?;

        self.rdma_read_mr(remote, &mut local_mr).await?;
        local.copy_from_slice(&local_mr.as_slice()[..local.len()]);
        Ok(())
    }

    /// RDMA WRITE using a pre-registered local memory region.
    ///
    /// This avoids per-operation memory registration (`ibv_reg_mr`) in the hot path.
    pub async fn rdma_write_mr(&self, remote: &RemoteBuf, local: &RdmaMr) -> io::Result<()> {
        validate_rdma_buffers(remote, local.as_slice().len(), "RDMA WRITE")?;

        let lkey = local.lkey();
        let local_addr = local.as_slice().as_ptr() as u64;
        let len = local.as_slice().len() as u32;

        let _permit = self
            .rdma_semaphore
            .acquire()
            .await
            .expect("Semaphore closed");
        let wr_id = self.alloc_wr_id();
        {
            let mut qp_guard = self.qp.lock().await;
            let mut guard = qp_guard.start_post_send();
            let wr = guard.construct_wr(wr_id, WorkRequestFlags::Signaled);
            unsafe {
                wr.setup_write(remote.rkey(), remote.addr())
                    .setup_sge(lkey, local_addr, len);
            }
            guard.post().map_err(|e| io::Error::other(e.to_string()))?;
        }

        let completion = self.wait_for_completion(wr_id).await?;
        if completion.status != WorkCompletionStatus::Success {
            return Err(io::Error::other(format!(
                "RDMA WRITE failed: status={:?}",
                completion.status
            )));
        }
        Ok(())
    }

    /// RDMA READ into a pre-registered local memory region.
    ///
    /// The read length is `local.as_slice().len()`.
    pub async fn rdma_read_mr(&self, remote: &RemoteBuf, local: &mut RdmaMr) -> io::Result<()> {
        validate_rdma_buffers(remote, local.as_slice().len(), "RDMA READ")?;

        let lkey = local.lkey();
        let local_addr = local.as_mut_slice().as_ptr() as u64;
        let len = local.as_slice().len() as u32;

        let _permit = self
            .rdma_semaphore
            .acquire()
            .await
            .expect("Semaphore closed");
        let wr_id = self.alloc_wr_id();
        {
            let mut qp_guard = self.qp.lock().await;
            let mut guard = qp_guard.start_post_send();
            let wr = guard.construct_wr(wr_id, WorkRequestFlags::Signaled);
            unsafe {
                wr.setup_read(remote.rkey(), remote.addr())
                    .setup_sge(lkey, local_addr, len);
            }
            guard.post().map_err(|e| io::Error::other(e.to_string()))?;
        }

        let completion = self.wait_for_completion(wr_id).await?;
        if completion.status != WorkCompletionStatus::Success {
            return Err(io::Error::other(format!(
                "RDMA READ failed: status={:?}",
                completion.status
            )));
        }
        Ok(())
    }
}
