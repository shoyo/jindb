/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::manager::{BufferManager, NoBufFrameErr};
use crate::common::{PageIdT, RecordIdT};
use crate::page::Page;
use crate::relation::record::Record;
use std::sync::Arc;

/// A heap is a collection of pages on disk which corresponds to a given relation.
/// Pages are connected together as a doubly linked list. Each page contains in its
/// header the IDs of its previous and next pages.
pub struct Heap {
    /// ID of the first page in the doubly linked list
    head_page_id: PageIdT,
}

impl Heap {
    /// Create a new heap for a database relation.
    pub fn new(buffer_manager: Arc<BufferManager>) -> Result<Self, NoBufFrameErr> {
        let page_latch = buffer_manager.create_relation_page()?;
        let head_page_id = match *page_latch.read().unwrap() {
            Some(ref page) => page.get_id(),
            None => panic!("Head page latch contained None"),
        };
        Ok(Self { head_page_id })
    }

    /// Insert a record into the relation.
    pub fn insert(&mut self, _record: Record) -> Result<(), ()> {
        Err(())
    }

    /// Flag the specified record as deleted.
    /// The record is not actually deleted until .apply_delete() is called.
    pub fn flag_delete(&mut self, _record_id: RecordIdT) -> Result<(), ()> {
        Err(())
    }

    /// Commit a delete operation for the specified record.
    pub fn commit_delete(&mut self, _record_id: RecordIdT) -> Result<(), ()> {
        Err(())
    }

    /// Rollback a delete operation for the specified record.
    pub fn rollback_delete(&mut self, _record_id: RecordIdT) -> Result<(), ()> {
        Err(())
    }
}
