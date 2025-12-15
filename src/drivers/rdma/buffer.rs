use crate::transport::BufferPool;
use crate::drivers::rdma::context::RdmaContext;
use bytes::BytesMut;
use sideway::ibverbs::memory_region::MemoryRegion;
use sideway::ibverbs::AccessFlags;
use std::sync::Arc;

#[allow(dead_code)]
pub struct RdmaBufferPool {
    context: Arc<RdmaContext>,
}

impl RdmaBufferPool {
    pub fn new(context: Arc<RdmaContext>) -> Self {
        Self { context }
    }
}

impl BufferPool for RdmaBufferPool {
    fn alloc(&self, size: usize) -> BytesMut {
        BytesMut::with_capacity(size)
    }
}

pub struct RdmaMr {
    pub mr: Arc<MemoryRegion>,
    buf: BytesMut,
}

impl RdmaMr {
    pub fn register(context: &RdmaContext, mut buf: BytesMut) -> Option<Self> {
        let len = buf.len();
        let ptr = buf.as_mut_ptr();
        let access = AccessFlags::LocalWrite | AccessFlags::RemoteWrite | AccessFlags::RemoteRead;

        // unsafe because we are registering raw pointer memory
        let mr = unsafe {
            context.pd.reg_mr(ptr as usize, len, access).ok()?
        };

        Some(Self { mr, buf })
    }

    pub fn lkey(&self) -> u32 {
        self.mr.lkey()
    }

    pub fn rkey(&self) -> u32 {
        self.mr.rkey()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    pub fn into_inner(self) -> BytesMut {
        self.buf
    }
}
