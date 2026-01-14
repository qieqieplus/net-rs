pub mod context;
pub mod buffer;
pub mod transport;
pub mod poller;
pub mod cm;

// Re-export from memory module
pub use crate::memory::{RemoteBuf, SlabAllocator, SlabChunk};
pub use transport::{RdmaTransport, TransportConfig};

use sideway::ibverbs::device::{DeviceList, DeviceInfo};

pub fn get_device_list() -> Vec<String> {
    match DeviceList::new() {
        Ok(list) => list.iter()
            .map(|d| d.name())
            .collect(),
        Err(_) => Vec::new(),
    }
}
