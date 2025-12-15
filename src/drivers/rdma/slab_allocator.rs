use crossbeam::queue::SegQueue;
use sideway::ibverbs::memory_region::MemoryRegion;
use sideway::ibverbs::protection_domain::ProtectionDomain;
use sideway::ibverbs::AccessFlags;
use std::io;
use std::sync::Arc;

/// A simple slab allocator backed by a set of pre-registered fixed-size chunks.
///
/// Each chunk owns its memory (a boxed slice) and is registered exactly once.
/// Allocation is lock-free via `SegQueue`. When a chunk is dropped, it is
/// automatically returned to its originating slab.
///
/// Note: This is intentionally conservative and avoids borrowing lifetimes so we
/// can hand out pool-backed `Bytes` using `Bytes::from_owner`.
pub struct SlabAllocator {
    enabled: bool,
    classes: Vec<SizeClass>,
}

struct SizeClass {
    chunk_size: usize,
    slots: Box<[ChunkSlot]>,
    free: SegQueue<usize>,
}

struct ChunkSlot {
    buf: Box<[u8]>,
    mr: Arc<MemoryRegion>,
    lkey: u32,
    rkey: u32,
}

impl SlabAllocator {
    /// Build a slab allocator from `(chunk_size, count)` size classes.
    ///
    /// If registration fails (e.g. locked-memory limit), return an `io::Error`.
    pub fn new(pd: &Arc<ProtectionDomain>, classes: &[(usize, usize)]) -> io::Result<Arc<Self>> {
        let mut out_classes = Vec::with_capacity(classes.len());
        let access = AccessFlags::LocalWrite | AccessFlags::RemoteWrite | AccessFlags::RemoteRead;

        for &(chunk_size, count) in classes {
            let mut slots: Vec<ChunkSlot> = Vec::with_capacity(count);
            let free = SegQueue::new();

            for idx in 0..count {
                let buf = vec![0u8; chunk_size].into_boxed_slice();
                let mr: Arc<MemoryRegion> = unsafe {
                    pd.reg_mr(buf.as_ptr() as usize, buf.len(), access)
                        .map_err(|e| io::Error::other(e.to_string()))?
                };
                slots.push(ChunkSlot {
                    lkey: mr.lkey(),
                    rkey: mr.rkey(),
                    mr,
                    buf,
                });
                free.push(idx);
            }

            out_classes.push(SizeClass {
                chunk_size,
                slots: slots.into_boxed_slice(),
                free,
            });
        }

        // Sort by increasing chunk size for "smallest-fit" allocation.
        out_classes.sort_by_key(|c| c.chunk_size);

        Ok(Arc::new(Self {
            enabled: true,
            classes: out_classes,
        }))
    }

    /// Default config approximating ~256MB total pinned memory.
    pub fn new_default(pd: &Arc<ProtectionDomain>) -> io::Result<Arc<Self>> {
        // (chunk_size, count)
        // 4KB * 4096 = 16MB
        // 64KB * 2048 = 128MB
        // 1MB * 128 = 128MB
        Self::new(
            pd,
            &[
                (4 * 1024 + 64, 4096),
                (64 * 1024 + 64, 2048),
                (1024 * 1024 + 64, 128),
            ],
        )
    }

    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            enabled: false,
            classes: Vec::new(),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Allocate a chunk with capacity >= `size`.
    pub fn alloc(self: &Arc<Self>, size: usize) -> Option<SlabChunk> {
        if !self.enabled {
            return None;
        }
        let (class_idx, class) = self
            .classes
            .iter()
            .enumerate()
            .find(|(_, c)| c.chunk_size >= size)?;
        let idx = class.free.pop()?;
        Some(SlabChunk {
            slab: Arc::clone(self),
            class_idx,
            idx,
        })
    }

    fn release(&self, class_idx: usize, idx: usize) {
        if !self.enabled {
            return;
        }
        if let Some(class) = self.classes.get(class_idx) {
            class.free.push(idx);
        }
    }
}

/// A leased chunk from a [`SlabAllocator`]. Returned to the slab on drop.
pub struct SlabChunk {
    slab: Arc<SlabAllocator>,
    class_idx: usize,
    idx: usize,
}

impl SlabChunk {
    pub fn capacity(&self) -> usize {
        self.slab.classes[self.class_idx].chunk_size
    }

    pub fn lkey(&self) -> u32 {
        self.slab.classes[self.class_idx].slots[self.idx].lkey
    }

    #[allow(dead_code)]
    pub fn rkey(&self) -> u32 {
        self.slab.classes[self.class_idx].slots[self.idx].rkey
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.slab.classes[self.class_idx].slots[self.idx].buf.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_ptr() as *mut u8
    }

    /// # Safety
    /// This is safe because `SlabChunk` is not `Clone`; the caller holds unique
    /// ownership of the chunk lease, so mutable access is exclusive.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = self.as_mut_ptr();
        let len = self.capacity();
        unsafe { std::slice::from_raw_parts_mut(ptr, len) }
    }

    pub fn as_slice(&self) -> &[u8] {
        let ptr = self.as_ptr();
        let len = self.capacity();
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    pub fn as_slot_mr(&self) -> Arc<MemoryRegion> {
        Arc::clone(&self.slab.classes[self.class_idx].slots[self.idx].mr)
    }
}

impl Drop for SlabChunk {
    fn drop(&mut self) {
        self.slab.release(self.class_idx, self.idx);
    }
}


