/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::manager::{BufferError, BufferManager};

use crate::common::{PageIdT, MAX_RECORD_SIZE};

use crate::page::relation_page::RelationPage;
use crate::page::{Page};
use crate::relation::record::{Record, RecordId};

use std::convert::From;
use std::sync::{Arc};

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

    /// Insert a record into the relation. If there is currently no space available in the buffer
    /// pool to fetch/create pages, return an error.
    ///
    /// This method traverses the doubly-linked list of pages until it encounters a page that has
    /// enough space to insert the record. If no page in the heap has enough space, we create a
    /// new page, insert the record, and append the new page to the end of the linked list.
    pub fn insert(&self, mut record: Record) -> Result<RecordId, HeapError> {
        // Assert that the record has not already been allocated and can fit in a page.
        if record.is_allocated() {
            return Err(HeapError::RecordAlreadyAlloc);
        }
        if record.len() > MAX_RECORD_SIZE {
            return Err(HeapError::RecordTooLarge);
        }

        // Traverse the heap.
        let mut page_id = self.head_page_id;
        loop {
            // 1) Fetch the current page and obtain a write latch.
            let frame_latch = match self.buffer_manager.fetch_page(page_id) {
                Ok(latch) => latch,
                Err(e) => match e {
                    BufferError::NoBufFrame => return Err(HeapError::from(e)),
                    BufferError::PageDiskDNE => {
                        panic!("All pages encountered while traversing heap should exist on disk")
                    }
                    _ => unreachable!(),
                },
            };
            let mut frame = frame_latch.write().unwrap();
            let page = frame
                .get_mut_page()
                .unwrap()
                .as_mut_any()
                .downcast_mut::<RelationPage>()
                .unwrap();

            // 2) Attempt to insert the record into the current page.
            // If the insertion was successful, return the newly initialized record ID.
            if page.insert_record(&mut record).is_ok() {
                self.buffer_manager.unpin_and_drop(frame);
                return Ok(record.get_id().unwrap());
            }

            // If the insertion was unsuccessful, we attempt to traverse to the next page. If
            // there is no next page, we instead create a new page, insert the record, and link
            // the new page to the end of the heap.
            match page.get_next_page_id() {
                Some(pid) => {
                    self.buffer_manager.unpin_and_drop(frame);
                    page_id = pid
                }
                None => {
                    let frame_latch = match self.buffer_manager.create_relation_page() {
                        Ok(latch) => latch,
                        Err(e) => match e {
                            BufferError::NoBufFrame => return Err(HeapError::from(e)),
                            _ => unreachable!(),
                        },
                    };
                    let mut new_frame = frame_latch.write().unwrap();
                    let new_page = new_frame
                        .get_mut_page()
                        .unwrap()
                        .as_mut_any()
                        .downcast_mut::<RelationPage>()
                        .unwrap();

                    new_page.insert_record(&mut record).unwrap();
                    new_page.set_prev_page_id(page.get_id());
                    page.set_next_page_id(new_page.get_id());

                    new_frame.set_dirty_flag(true);
                    frame.set_dirty_flag(true);

                    self.buffer_manager.unpin_and_drop(new_frame);
                    self.buffer_manager.unpin_and_drop(frame);

                    return Ok(record.get_id().unwrap());
                }
            }
        }
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
