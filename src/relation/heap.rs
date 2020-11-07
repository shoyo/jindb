use crate::buffer::manager::BufferManager;
use crate::common::constants::BlockIdT;

/// A heap is a collection of blocks on disk which corresponds to a given relation.
/// Blocks are chained together as a doubly linked list. Each block contains in its
/// header the IDs of its previous and next blocks.
pub struct Heap {
    /// ID of the first block in the doubly linked list
    head_block_id: BlockIdT,

    /// Buffer manager instance handling the blocks for this relation
    buffer_manager: BufferManager,
}

impl Heap {
    pub fn new(head_block_id: BlockIdT, buffer_manager: BufferManager) -> Self {
        Self {
            head_block_id,
            buffer_manager,
        }
    }
}
