use crate::storage::block::Block;
use std::collections::HashMap;

/// The buffer manager is responsible for fetching/flushing blocks
/// that are managed in memory.
pub struct BufferManager {
    /// Collection of buffer frames that can hold blocks
    buffer_pool: Vec<BufferFrame>,
    /// Mapping between blocks and the frames that hold them
    block_table: HashMap<u32, u32>,
}

impl BufferManager {
    pub fn new(size: u32) -> Self {
        let mut pool = Vec::new();
        for i in 0..size {
            pool.push(BufferFrame::new(i))
        }
        Self {
            buffer_pool: pool,
            block_table: HashMap::new(),
        }
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_pool.len()
    }

    pub fn fetch_block(&mut self, block_id: u32) -> Option<Block> {
        None
    }

    pub fn flush_block(&mut self, block_id: u32) -> Result<(), ()> {
        Ok(())
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
