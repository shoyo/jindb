/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::manager::BufferManager;
use crate::common::{PageIdT, RecordIdT};
use crate::relation::record::Record;

/// A heap is a collection of pages on disk which corresponds to a given relation.
/// Pages are connected together as a doubly linked list. Each page contains in its
/// header the IDs of its previous and next pages.
pub struct Heap {
    /// ID of the first page in the doubly linked list
    head_page_id: PageIdT,
}

impl Heap {
    /// Create a new heap for a database relation.
    pub fn new(buffer_manager: &mut BufferManager) -> Result<Self, String> {
        let rwlatch = match buffer_manager.create_page() {
            Ok(latch) => latch,
            Err(_) => {
                return Err(format!(
                    "Failed to initialize the head page for a relation heap"
                ))
            }
        };
        let head_page_id = match *rwlatch.read().unwrap() {
            Some(ref page) => page.id,
            None => panic!("Head page latch contained None"),
        };
        buffer_manager.unpin_page(head_page_id).unwrap();
        Ok(Self { head_page_id })
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
