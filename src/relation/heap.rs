use crate::buffer::manager::BufferManager;
use crate::common::constants::{BlockIdT, RecordIdT};
use crate::relation::record::Record;

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
    /// Create a new heap for a database relation.
    pub fn new(head_block_id: BlockIdT, buffer_manager: BufferManager) -> Self {
        Self {
            head_block_id,
            buffer_manager,
        }
    }

    /// Insert a record into the relation.
    pub fn insert(record: Record) -> Result<(), ()> {
        Err(())
    }

    /// Mark the specified record as deleted.
    /// The record is not actually deleted until .apply_delete() is called.
    pub fn mark_delete(record_id: RecordIdT) -> Result<(), ()> {
        Err(())
    }

    /// Apply a delete operation to the specified record.
    pub fn apply_delete(record_id: RecordIdT) -> Result<(), ()> {
        Err(())
    }

    /// Rollback a delete operation.
    pub fn rollback_delete(record_id: RecordIdT) -> Result<(), ()> {
        Err(())
    }
}
