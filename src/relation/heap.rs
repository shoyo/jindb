use crate::buffer::manager::BufferManager;
use crate::common::constants::{BlockIdT, RecordIdT};
use crate::relation::record::Record;

/// A heap is a collection of blocks on disk which corresponds to a given relation.
/// Blocks are connected together as a doubly linked list. Each block contains in its
/// header the IDs of its previous and next blocks.
pub struct Heap {
    /// ID of the first block in the doubly linked list
    head_block_id: BlockIdT,
}

impl Heap {
    /// Create a new heap for a database relation.
    pub fn new(buffer_manager: &mut BufferManager) -> Result<Self, String> {
        let rwlatch = match buffer_manager.create_block() {
            Ok(latch) => latch,
            Err(_) => {
                return Err(format!(
                    "Failed to initialize the head block for a relation heap"
                ))
            }
        };
        let head_block_id = match *rwlatch.read().unwrap() {
            Some(ref block) => block.id,
            None => panic!("Head block latch contained None"),
        };
        buffer_manager.unpin_block(head_block_id).unwrap();
        Ok(Self {
            head_block_id: head_block_id,
        })
    }

    /// Insert a record into the relation.
    pub fn insert(&mut self, record: Record) -> Result<(), ()> {
        Err(())
    }

    /// Flag the specified record as deleted.
    /// The record is not actually deleted until .apply_delete() is called.
    pub fn flag_delete(&mut self, record_id: RecordIdT) -> Result<(), ()> {
        Err(())
    }

    /// Commit a delete operation for the specified record.
    pub fn commit_delete(&mut self, record_id: RecordIdT) -> Result<(), ()> {
        Err(())
    }

    /// Rollback a delete operation for the specified record.
    pub fn rollback_delete(&mut self, record_id: RecordIdT) -> Result<(), ()> {
        Err(())
    }
}
