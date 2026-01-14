use sideway::ibverbs::device_context::DeviceContext;
use sideway::ibverbs::device::{DeviceList, DeviceInfo};
use sideway::ibverbs::protection_domain::ProtectionDomain;
use std::io;
use std::sync::Arc;
use tracing::warn;

use crate::memory::SlabAllocator;

pub struct RdmaContext {
    pub(crate) ctx: Arc<DeviceContext>,
    pub(crate) pd: Arc<ProtectionDomain>,
    pub(crate) slab: Arc<SlabAllocator>,
}

impl RdmaContext {
    pub fn open(dev_name: &str) -> io::Result<Arc<Self>> {
        let device_list = DeviceList::new()
            .map_err(|e| io::Error::other(e.to_string()))?;

        let device = device_list.iter()
            .find(|d| d.name() == dev_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Device {} not found", dev_name)))?;

        let ctx = device.open()
            .map_err(|e| io::Error::other(e.to_string()))?;

        let pd = ctx.alloc_pd()
            .map_err(|e| io::Error::other(e.to_string()))?;

        let slab = match SlabAllocator::new_default(&pd) {
            Ok(s) => s,
            Err(e) => {
                warn!("RDMA slab allocator disabled (failed to register pool): {e}");
                SlabAllocator::disabled()
            }
        };

        // device.open() and alloc_pd() already return Arc-wrapped types
        Ok(Arc::new(Self { ctx, pd, slab }))
    }

    pub fn from_device_context(ctx: Arc<DeviceContext>) -> io::Result<Arc<Self>> {
        let pd = ctx
            .alloc_pd()
            .map_err(|e| io::Error::other(e.to_string()))?;

        let slab = match SlabAllocator::new_default(&pd) {
            Ok(s) => s,
            Err(e) => {
                warn!("RDMA slab allocator disabled (failed to register pool): {e}");
                SlabAllocator::disabled()
            }
        };

        Ok(Arc::new(Self { ctx, pd, slab }))
    }
}
