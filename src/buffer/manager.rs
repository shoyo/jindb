/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::block::table_block::RelationBlock;
use crate::buffer::eviction_policies::clock::ClockPolicy;
use crate::buffer::eviction_policies::policy::Policy;
use crate::common::{BlockIdT, BufferFrameIdT};
use crate::disk::manager::DiskManager;
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, Mutex, RwLock};

/// Type alias for a block protected by a R/W latch for concurrent access.
type BlockLatch = Arc<RwLock<Option<RelationBlock>>>;

/// The buffer manager is responsible for fetching/flushing blocks that are
/// managed in memory. Any blocks that don't exist in the buffer are retrieved
/// from disk through the disk manager.
pub struct BufferManager {
    /// Number of buffer frames in the buffer pool
    buffer_size: BufferFrameIdT,

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
    /// The length of the vector should never change and should always be equal
    /// to self.buffer_size.
    buffer_pool: Vec<BlockLatch>,

    /// Mapping from block IDs to buffer frame IDs
    block_table: Arc<Mutex<HashMap<BlockIdT, BufferFrameIdT>>>,

    /// Disk manager to access blocks on disk
    disk_manager: DiskManager,

    /// List of frame IDs that are not occupied
    free_list: Arc<Mutex<LinkedList<BufferFrameIdT>>>,

    /// Buffer eviction policy
    policy: ClockPolicy,
}

impl BufferManager {
    /// Construct a new buffer manager.
    pub fn new(disk_manager: DiskManager, buffer_size: BufferFrameIdT) -> Self {
        let mut pool: Vec<BlockLatch> = Vec::with_capacity(buffer_size as usize);
        let mut free_list: LinkedList<BufferFrameIdT> = LinkedList::new();
        for frame_id in 0..buffer_size {
            pool.push(Arc::new(RwLock::new(None)));
            free_list.push_back(frame_id);
        }
        Self {
            buffer_size,
            buffer_pool: pool,
            block_table: Arc::new(Mutex::new(HashMap::new())),
            disk_manager,
            free_list: Arc::new(Mutex::new(free_list)),
            policy: ClockPolicy::new(),
        }
    }

    /// Initialize a new block, pin it, and return the block latch.
    /// If there are no open buffer frames and all existing blocks are pinned, then
    /// return an error.
    pub fn create_block(&mut self) -> Result<BlockLatch, ()> {
        // Allocate space in disk and initialize the new block.
        let block_id = self.disk_manager.allocate_block();
        let block = RelationBlock::new(block_id);
        let block_latch = Arc::new(RwLock::new(Some(block)));

        // Find a frame in the buffer to house the newly created block.
        // Starting by checking the free list, which is a list of open frame IDs.
        let mut list = self.free_list.lock().unwrap();
        if list.is_empty() {
            // If free list is empty, then scan buffer frames for an unpinned block
            for i in 0..self.buffer_size {}
        } else {
            // If the free list is not empty, then pop off an index and pin the block
            // to the corresponding frame. Be sure to wrap the block in a block latch.
            let open_frame_id = list.len();
            let mut frame = self.buffer_pool[open_frame_id].write().unwrap();
        }

        Ok(block_latch.clone())
    }

    /// Fetch the specified block, pin it, and return the block latch.
    /// If the block does not exist in the buffer, then fetch the block from disk.
    /// If the block does not exist on disk, then return an error.
    pub fn fetch_block(&mut self, block_id: BlockIdT) -> Result<BlockLatch, ()> {
        match self._block_table_lookup(block_id) {
            // Block currently exists in the buffer, so pin it and return the latch.
            Some(frame_id) => {
                let latch = self._get_block_by_frame(frame_id).unwrap();
                let latch = match *latch.write().unwrap() {
                    Some(ref mut block) => {
                        block.pin_count += 1;
                        Ok(latch.clone())
                    }
                    None => panic!(
                        "Specified block ID {} points to an empty buffer frame",
                        block_id
                    ),
                };
                latch
            }
            // Block does not currently exist in the buffer, so fetch the block from disk.
            None => todo!(),
        }
    }

    /// Delete the specified block.
    /// If the block is pinned, then return an error.
    pub fn delete_block(&mut self, block_id: BlockIdT) -> Result<(), ()> {
        Err(())
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
        let frame_id = self._block_table_lookup(block_id).unwrap();
        let latch = self._get_block_by_frame(frame_id).unwrap();
        match *latch.write().unwrap() {
            Some(ref mut block) => block.pin_count += 1,
            None => panic!(
                "Attempted to pin a block contained in a block latch, but the latch contained None."
            ),
        }
        Ok(())
    }

    /// Unpin the specified block.
    /// Blocks with no pins can be evicted. Threads must unpin a block when
    /// finished operating on it.
    pub fn unpin_block(&self, block_id: BlockIdT) -> Result<(), String> {
        let frame_id = self._block_table_lookup(block_id).unwrap();
        let latch = self._get_block_by_frame(frame_id).unwrap();
        match *latch.write().unwrap() {
            Some(ref mut block) => {
                if block.pin_count == 0 {
                    return Err(format!("Attempted to unpin a block with a pin count of 0."));
                }
                block.pin_count -= 1;
            }
            None => panic!("Attempted to unpin a block contained in a block latch, but the latch contained None."),
        }
        Ok(())
    }

    /// Index the buffer pool and return the specified block latch.
    fn _get_block_by_frame(&self, frame_id: BufferFrameIdT) -> Result<BlockLatch, String> {
        let latch = self.buffer_pool[frame_id as usize].clone();
        Ok(latch)
    }

    /// Find the specified block in the block table, and return its frame ID.
    /// If the block does not exist in the block table, then return None.
    /// Panic if the frame ID is out-of-bounds.
    fn _block_table_lookup(&self, block_id: BlockIdT) -> Option<BufferFrameIdT> {
        let table = self.block_table.lock().unwrap();
        match table.get(&block_id) {
            Some(frame_id) => {
                if *frame_id >= self.buffer_size {
                    panic!(format!(
                        "Frame ID {} out of range (buffer size = {}) [broken block table]",
                        frame_id, self.buffer_size
                    ));
                }
                Some(*frame_id)
            }
            None => None,
        }
    }
}
