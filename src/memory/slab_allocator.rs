use crossbeam::queue::SegQueue;
use sideway::ibverbs::memory_region::MemoryRegion;
use sideway::ibverbs::protection_domain::ProtectionDomain;
use sideway::ibverbs::AccessFlags;
use std::io;
use std::sync::Arc;

/// A simple slab allocator backed by a set of pre-registered fixed-size chunks.
///
/// Each size class has a single contiguous memory region (MR) that is sliced into chunks.
/// Allocation is lock-free via `SegQueue`. When a chunk is dropped, it is
/// automatically returned to its originating slab.
pub struct SlabAllocator {
    enabled: bool,
    classes: Vec<SizeClass>,
}

struct SizeClass {
    chunk_size: usize,
    base_ptr: usize,
    mr: Arc<MemoryRegion>,
    // The backing memory must be kept alive.
    // We use a Box<[u8]> but we need to access it mutably from multiple threads
    // (disjoint chunks). We use `UnsafeCell` semantics effectively by holding the raw pointer
    // and ensuring exclusive access via the `free` queue.
    _backing_mem: Box<[u8]>,
    free: SegQueue<usize>,
}

// SAFETY: SizeClass contains a raw pointer (`base_ptr`) and backing memory that is accessed
// from multiple threads. We implement Send+Sync manually because:
//
// 1. EXCLUSIVE CHUNK OWNERSHIP: The `free: SegQueue<usize>` ensures that each chunk index
//    is owned by exactly one thread at a time. `SegQueue::pop()` atomically removes an index,
//    giving the caller exclusive ownership until `push()` returns it.
//
// 2. NON-CLONEABLE CHUNKS: `SlabChunk` is not Clone, so there's no way to duplicate ownership.
//    A chunk index can only be "in the free queue" OR "owned by a SlabChunk", never both.
//
// 3. MUTABLE ACCESS REQUIRES MUT: `SlabChunk::as_mut_slice(&mut self)` requires exclusive
//    borrow of the chunk, ensuring no concurrent mutable access to the same memory region.
//
// 4. DROP RETURNS OWNERSHIP: `SlabChunk::drop()` pushes the index back to the free queue,
//    completing the ownership cycle. The chunk's memory is not accessed after drop.
//
// 5. IMMUTABLE METADATA: `chunk_size`, `base_ptr`, and `mr` are immutable after construction.
//    Multiple threads can safely read these values concurrently.
//
// 6. MEMORY REGION LIFETIME: `_backing_mem: Box<[u8]>` owns the memory and lives as long as
//    the SizeClass. The MR registration is valid for this lifetime.
//
// INVARIANT (soundness-critical): A chunk index `idx` is either:
//   (a) In the free queue: No code holds a pointer/reference to chunk memory at `base + idx * size`
//   (b) Owned by exactly one SlabChunk: Only that SlabChunk may access the memory
//
// Breaking this invariant (e.g., double-push, use-after-free) would cause undefined behavior.
unsafe impl Send for SizeClass {}
unsafe impl Sync for SizeClass {}


impl SlabAllocator {
    /// Build a slab allocator from `(chunk_size, count)` size classes.
    ///
    /// If registration fails (e.g. locked-memory limit), return an `io::Error`.
    pub fn new(pd: &Arc<ProtectionDomain>, classes: &[(usize, usize)]) -> io::Result<Arc<Self>> {
        let mut out_classes = Vec::with_capacity(classes.len());
        let access = AccessFlags::LocalWrite | AccessFlags::RemoteWrite | AccessFlags::RemoteRead;

        for &(chunk_size, count) in classes {
            // Allocate one large buffer for the entire class
            let total_size = chunk_size.checked_mul(count).ok_or_else(|| {
                io::Error::other("slab size overflow")
            })?;

            // Initialize with zeros
            let mut backing_mem = vec![0u8; total_size].into_boxed_slice();

            // Register the entire region
            let mr = unsafe {
                pd.reg_mr(backing_mem.as_mut_ptr() as usize, total_size, access)
                    .map_err(|e| io::Error::other(e.to_string()))?
            };
            // pd.reg_mr returns Arc<MemoryRegion>

            let free = SegQueue::new();
            for idx in 0..count {
                free.push(idx);
            }

            out_classes.push(SizeClass {
                chunk_size,
                base_ptr: backing_mem.as_ptr() as usize,
                _backing_mem: backing_mem,
                mr,
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
        self.slab.classes[self.class_idx].mr.lkey()
    }

    #[allow(dead_code)]
    pub fn rkey(&self) -> u32 {
        self.slab.classes[self.class_idx].mr.rkey()
    }

    pub fn as_ptr(&self) -> *const u8 {
        let class = &self.slab.classes[self.class_idx];
        let offset = self.idx * class.chunk_size;
        (class.base_ptr + offset) as *const u8
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_ptr() as *mut u8
    }

    /// # Safety
    /// This is safe because `SlabChunk` is not `Clone`; the caller holds unique
    /// ownership of the chunk lease, so mutable access is exclusive (managed by SegQueue).
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

    // Used by tests or internals
    pub fn as_slot_mr(&self) -> Arc<MemoryRegion> {
        Arc::clone(&self.slab.classes[self.class_idx].mr)
    }
}

impl Drop for SlabChunk {
    fn drop(&mut self) {
        self.slab.release(self.class_idx, self.idx);
    }
}
