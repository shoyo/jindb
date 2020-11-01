use crate::storage::block::Block;
use crate::storage::disk_manager::DiskManager;
use std::collections::HashMap;

/// The buffer manager is responsible for fetching/flushing blocks that are
/// managed in memory. Any blocks that don't exist in the buffer are retrieved
/// from disk through the disk manager.
pub struct BufferManager {
    /// Collection of buffer frames that can hold blocks
    buffer_pool: Vec<Option<Block>>,
    /// Mapping from block ids to buffer frame ids
    block_table: HashMap<u32, u32>,
    /// Disk manager to access blocks on disk
    disk_manager: DiskManager,
}

impl BufferManager {
    /// Construct a new buffer manager.
    pub fn new(buffer_size: u32, disk_manager: DiskManager) -> Self {
        let mut pool: Vec<Option<Block>> = Vec::new();
        for _ in 0..buffer_size as usize {
            pool.push(None);
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

    /// Search the buffer pool for a block by its id and return a mutable reference.
    fn get_block_by_id(&self, block_id: u32) -> Option<&mut Block> {
        None
    }

    /// Fetch the specified block from the buffer and return the block if
    /// successful.
    pub fn fetch_block(&mut self, block_id: u32) -> Option<Block> {
        let block = match self.block_table.get(&block_id) {
            Some(idx) => &self.buffer_pool[*idx as usize],
            None => return None,
        };
        self.pin_block(block_id);
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
