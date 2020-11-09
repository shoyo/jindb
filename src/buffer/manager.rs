use crate::block::table_block::TableBlock;
use crate::common::constants::{BlockIdT, BufferFrameIdT, BUFFER_SIZE};
use crate::disk::manager::DiskManager;
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, RwLock};

/// Type alias for a block protected by a R/W latch for concurrent access.
type BlockLatch = Arc<RwLock<TableBlock>>;

/// The buffer manager is responsible for fetching/flushing blocks that are
/// managed in memory. Any blocks that don't exist in the buffer are retrieved
/// from disk through the disk manager.
pub struct BufferManager {
    /// Collection of buffer frames that can hold guarded blocks
    ///
    /// Note:
    /// Buffer pool is defined as a vector instead of a fixed-size array
    /// because of limitations with Rust syntax.
    /// To *safely* declare an N-length array of Option<T>, the options are:
    ///     1) [None; N] syntax, which requires T to implement
    ///        std::marker::Copy.
    ///     2) Using Default::default(), which requires N <= 32.
    /// Because of these limitations, the buffer pool is defined as a Vec type.
    /// Despite this, the length of the vector should never change and should
    /// always have a length of common::constants::BUFFER_SIZE.
    buffer_pool: Vec<Option<BlockLatch>>,

    /// Mapping from block IDs to buffer frame IDs
    block_table: HashMap<BlockIdT, BufferFrameIdT>,

    /// Disk manager to access blocks on disk
    disk_manager: DiskManager,

    /// List of frame IDs that are not occupied
    free_list: LinkedList<BufferFrameIdT>,
}

impl BufferManager {
    /// Construct a new buffer manager.
    pub fn new(disk_manager: DiskManager) -> Self {
        let mut pool: Vec<Option<BlockLatch>> = Vec::with_capacity(BUFFER_SIZE as usize);
        let mut free: LinkedList<BufferFrameIdT> = LinkedList::new();
        for i in 0..BUFFER_SIZE as usize {
            pool.push(None);
            free.push_back(i as BufferFrameIdT);
        }
        Self {
            buffer_pool: pool,
            block_table: HashMap::new(),
            disk_manager,
            free_list: free,
        }
    }

    /// Return the size of the buffer pool.
    pub fn buffer_size(&self) -> BufferFrameIdT {
        self.buffer_pool.len() as BufferFrameIdT
    }

    /// Initialize a new block and return the block if successful.
    pub fn new_block(&mut self) -> Option<BlockLatch> {
        let block_id = self.disk_manager.allocate_block();
        None
    }

    /// Fetch the specified block from the buffer, pin it, and return the block
    /// if successful.
    pub fn fetch_block_latch(&mut self, block_id: BlockIdT) -> Option<BlockLatch> {
        match self.get_frame_id(block_id) {
            Some(frame_id) => {
                let latch = self.get_block_latch_by_frame_id(frame_id).unwrap();
                let mut block = latch.write().unwrap();
                block.pin_count += 1;
                Some(Arc::clone(&latch))
            }
            None => None,
        }
    }

    /// Flush the specified block to disk.
    pub fn flush_block(&mut self, block_id: BlockIdT) -> Result<(), ()> {
        Err(())
    }

    /// Flush all blocks to disk.
    pub fn flush_all_blocks(&mut self) -> Result<(), ()> {
        Err(())
    }

    /// Pin the specified block to the buffer.
    /// Pinned blocks will never be evicted. Threads must pin a block to the
    /// buffer before operating on it.
    pub fn pin_block(&self, block_id: BlockIdT) -> Result<(), ()> {
        let frame_id = self.get_frame_id(block_id).unwrap();
        let latch = self.get_block_latch_by_frame_id(frame_id).unwrap();
        let mut block = latch.write().unwrap();
        block.pin_count += 1;
        Ok(())
    }

    /// Unpin the specified block.
    /// Blocks with no pins can be evicted. Threads must unpin a block when
    /// finished operating on it.
    pub fn unpin_block(&self, block_id: BlockIdT) -> Result<(), String> {
        let frame_id = self.get_frame_id(block_id).unwrap();
        let latch = self.get_block_latch_by_frame_id(frame_id).unwrap();
        let mut block = latch.write().unwrap();
        if block.pin_count == 0 {
            return Err(format!("Attempted to unpin a block with a pin count of 0."));
        }
        block.pin_count -= 1;
        Ok(())
    }

    /// Index the buffer pool and return the specified block latch.
    /// Performs error handling for cases such as out-of-bounds and empty frames.
    fn get_block_latch_by_frame_id(&self, frame_id: BufferFrameIdT) -> Result<BlockLatch, String> {
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
    fn get_frame_id(&self, block_id: BlockIdT) -> Option<BufferFrameIdT> {
        match self.block_table.get(&block_id) {
            Some(frame_id) => Some(*frame_id),
            None => None,
        }
    }
}
