/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::manager::{BufferError, BufferManager};
use crate::buffer::BufferFrame;
use crate::common::{PageIdT, PAGE_SIZE};
use crate::page::relation_page::RelationPage;
use crate::page::Page;
use crate::relation::record::{Record, RecordId};
use std::convert::From;
use std::sync::{Arc, RwLock};

/// A heap is a collection of pages on disk which corresponds to a given relation.
/// Pages are connected together as a doubly linked list. Each page contains in its
/// header the IDs of its previous and next pages.
pub struct Heap {
    /// ID of the first page in the doubly linked list.
    head_page_id: PageIdT,

    /// Buffer manager to request necessary pages for relation operations.
    buffer_manager: Arc<BufferManager>,
}

impl Heap {
    /// Create a new heap for a database relation.
    pub fn new(buffer_manager: Arc<BufferManager>) -> Result<Self, BufferError> {
        let frame_latch = buffer_manager.create_relation_page()?;
        let frame = frame_latch.read().unwrap();
        let head_page_id = match frame.get_page() {
            Some(ref page) => page.get_id(),
            None => panic!("Head frame latch contained no page"),
        };

        Ok(Self {
            head_page_id,
            buffer_manager,
        })
    }

    /// Insert a record into the relation.
    pub fn insert(&self, record: Record) -> Result<RecordId, HeapError> {
        if record.len() > PAGE_SIZE {
            return Err(HeapError::RecordTooLarge);
        }
        if record.is_allocated() {
            return Err(HeapError::RecordAlreadyAlloc);
        }

        let frame_latch = self.buffer_manager.fetch_page(self.head_page_id)?;
        let mut frame = frame_latch.write().unwrap();

        while let Some(rpage) = match frame.get_mut_page() {
            Some(page) => Some(page.as_mut_any().downcast_mut::<RelationPage>().unwrap()),
            None => None,
        } {
            todo!();
        }

        Ok(record.get_id().unwrap())
    }

    /// Update a record in this relation.
    pub fn update(&self, _record: Record) -> Result<(), ()> {
        Err(())
    }

    /// Flag the specified record as deleted.
    /// The record is not actually deleted until .apply_delete() is called.
    pub fn flag_delete(&self, _record_id: RecordId) -> Result<(), ()> {
        Err(())
    }

    /// Commit a delete operation for the specified record.
    pub fn commit_delete(&self, _record_id: RecordId) -> Result<(), ()> {
        Err(())
    }

    /// Rollback a delete operation for the specified record.
    pub fn rollback_delete(&self, _record_id: RecordId) -> Result<(), ()> {
        Err(())
    }
}

struct HeapIterator {}

/// Custom errors to be used by the heap.
#[derive(Debug)]
pub enum HeapError {
    /// Error to be thrown when a record to be used for insertion or replacement is already
    /// allocated elsewhere on disk.
    RecordAlreadyAlloc,

    /// Error to be thrown when a record is too large to be inserted into the database.
    /// This error should eventually become obsolete once records of arbitrary size become
    /// supported.
    RecordTooLarge,

    /// Errors to be thrown when the buffer manager encounters a recoverable error.
    BufMgrNoBufFrame,
    BufMgrPagePinned,
    BufMgrPageBufDNE,
    BufMgrPageDiskDNE,
}

impl From<BufferError> for HeapError {
    fn from(e: BufferError) -> Self {
        match e {
            BufferError::NoBufFrame => HeapError::BufMgrNoBufFrame,
            BufferError::PagePinned => HeapError::BufMgrPagePinned,
            BufferError::PageBufDNE => HeapError::BufMgrPageBufDNE,
            BufferError::PageDiskDNE => HeapError::BufMgrPageDiskDNE,
        }
    }
}
