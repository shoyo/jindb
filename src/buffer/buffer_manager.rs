use crate::storage::block::Block;
use crate::storage::disk_manager::DiskManager;
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, RwLock};

/// Type alias for a block protected by a R/W latch for concurrent access.
type BlockLatch = Arc<RwLock<Block>>;

/// The buffer manager is responsible for fetching/flushing blocks that are
/// managed in memory. Any blocks that don't exist in the buffer are retrieved
/// from disk through the disk manager.
pub struct BufferManager {
    /// Collection of buffer frames that can hold guarded blocks
    buffer_pool: Vec<Option<BlockLatch>>,

    /// Mapping from block ids to buffer frame ids
    block_table: HashMap<u32, u32>,

    /// Disk manager to access blocks on disk
    disk_manager: DiskManager,

    /// List of frame IDs that are not occupied
    free_list: LinkedList<u32>,
}

impl BufferManager {
    /// Construct a new buffer manager.
    pub fn new(buffer_size: u32, disk_manager: DiskManager) -> Self {
        let mut pool: Vec<Option<BlockLatch>> = Vec::new();
        let mut free: LinkedList<u32> = LinkedList::new();
        for i in 0..buffer_size as usize {
            pool.push(None);
            free.push_back(i as u32);
        }
        Self {
            buffer_pool: pool,
            block_table: HashMap::new(),
            disk_manager,
            free_list: free,
        }
    }

    /// Return the size of the buffer pool.
    pub fn buffer_size(&self) -> u32 {
        self.buffer_pool.len() as u32
    }

    /// Initialize a new block and return the block if successful.
    pub fn new_block(&mut self) -> Option<Block> {
        None
    }

    /// Fetch the specified block from the buffer, pin it, and return the block
    /// if successful.
    pub fn fetch_block_latch(&mut self, block_id: u32) -> Option<Block> {
        // match self.get_frame_id(block_id) {
        //     Some(frame_id) => {
        //         let latch = self.get_block_latch_by_frame_id(frame_id).unwrap();
        //         let mut block = latch.write().unwrap();
        //         block.pin_count += 1;
        //         Some()
        //     }
        //     None => None,
        // }
        None
    }

    /// Flush the specified block to disk.
    pub fn flush_block(&mut self, block_id: u32) -> Result<(), ()> {
        Err(())
    }

    /// Flush all blocks to disk.
    pub fn flush_all_blocks(&mut self) -> Result<(), ()> {
        Err(())
    }

    /// Pin the specified block to the buffer.
    /// Pinned blocks will never be evicted. Threads must pin a block to the
    /// buffer before operating on it.
    pub fn pin_block(&self, block_id: u32) -> Result<(), ()> {
        let frame_id = self.get_frame_id(block_id).unwrap();
        let latch = self.get_block_latch_by_frame_id(frame_id).unwrap();
        let mut block = latch.write().unwrap();
        block.pin_count += 1;
        Ok(())
    }

    /// Unpin the specified block.
    /// Blocks with no pins can be evicted. Threads must unpin a block when
    /// finished operating on it.
    pub fn unpin_block(&self, block_id: u32) -> Result<(), ()> {
        let frame_id = self.get_frame_id(block_id).unwrap();
        let latch = self.get_block_latch_by_frame_id(frame_id).unwrap();
        let mut block = latch.write().unwrap();
        block.pin_count -= 1;
        Ok(())
    }

    /// Index the buffer pool and return the specified block latch.
    /// Performs error handling for cases such as out-of-bounds and empty frames.
    fn get_block_latch_by_frame_id(&self, frame_id: u32) -> Result<BlockLatch, String> {
        if frame_id >= self.buffer_size() {
            return Err(format!(
                "Frame ID {} out of range (buffer size = {}) [broken block table]",
                frame_id,
                self.buffer_size()
            ));
        }
        match &self.buffer_pool[frame_id as usize] {
            Some(latch) => Ok(Arc::clone(latch)),
            None => Err(format!(
                "Frame ID {} points to empty buffer frame [broken block table]",
                frame_id
            )),
        }
    }

    /// Look up the frame ID of the specified block ID in the block table.
    /// Return a value instead of a reference (which is the default for
    /// std::collections::HashMap).
    fn get_frame_id(&self, block_id: u32) -> Option<u32> {
        match self.block_table.get(&block_id) {
            Some(frame_id) => Some(*frame_id),
            None => None,
        }
    }
}
