use crate::storage::block::Block;
use crate::storage::disk_manager::DiskManager;
use std::collections::HashMap;

/// The buffer manager is responsible for fetching/flushing blocks that are
/// managed in memory. Any blocks that don't exist in the buffer are retrieved
/// from disk through the disk manager.
pub struct BufferManager {
    /// Collection of buffer frames that can hold blocks
    buffer_pool: Vec<BufferFrame>,
    /// Mapping between blocks and the frames that hold them
    block_table: HashMap<u32, u32>,
    /// Disk manager to access blocks on disk
    disk_manager: DiskManager,
}

impl BufferManager {
    /// Construct a new buffer manager.
    pub fn new(buffer_size: u32, disk_manager: DiskManager) -> Self {
        let mut pool = Vec::new();
        for i in 0..buffer_size {
            pool.push(BufferFrame::new(i))
        }
        Self {
            buffer_pool: pool,
            block_table: HashMap::new(),
            disk_manager,
        }
    }

    /// Return the size of the buffer pool.
    pub fn buffer_size(&self) -> usize {
        self.buffer_pool.len()
    }

    /// Fetch the specified block from the buffer and return the block if
    /// successful.
    pub fn fetch_block(&mut self, block_id: u32) -> Option<Block> {
        None
    }

    /// Flush the specified block to disk.
    pub fn flush_block(&mut self, block_id: u32) -> Result<(), ()> {
        Err(())
    }

    /// Pin the specified block to the buffer.
    /// Pinned blocks will never be evicted. Concurrent threads must pin a
    /// block to the buffer before operating on it.
    pub fn pin_block(&self, block_id: u32) -> Result<(), ()> {
        Err(())
    }

    /// Unpin the specified block from the buffer.
    /// Unpinned blocks can be evicted. Concurrent threads must unpin a
    /// block when finished operating on it.
    pub fn unpin_block(&self, block_id: u32) -> Result<(), ()> {
        Err(())
    }

    /// Allocate space for a new block and return the block if successful.
    pub fn new_block(&self, block_id: u32) -> Option<Block> {
        None
    }
}

struct BufferFrame {
    id: u32,
    block: Option<Block>,
}

impl BufferFrame {
    pub fn new(id: u32) -> Self {
        Self { id, block: None }
    }
}
