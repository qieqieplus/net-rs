pub mod context;
pub mod buffer;
pub mod transport;
pub mod poller;
pub mod cm;
pub mod slab_allocator;
pub mod recv_pool;

use sideway::ibverbs::device::{DeviceList, DeviceInfo};

pub fn get_device_list() -> Vec<String> {
    match DeviceList::new() {
        Ok(list) => list.iter()
            .map(|d| d.name())
            .collect(),
        Err(_) => Vec::new(),
    }
}
