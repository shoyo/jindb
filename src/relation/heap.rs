/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jindb for more information about this project and its license.
 */

use crate::buffer::{BufferError, BufferManager};
use crate::constants::{PageIdT, MAX_RECORD_SIZE};

use crate::relation::record::{Record, RecordId};

use crate::page::{PageError, RelationPage};

use std::convert::From;
use std::sync::Arc;

/// A heap is a collection of pages on disk which corresponds to a given relation.
/// Pages are connected together as a doubly linked list. Each page contains in its
/// header the IDs of its previous and next pages.
pub struct Heap {
    /// ID of the first page in the doubly linked list.
    root_id: PageIdT,

    /// Buffer manager to request necessary pages for relation operations.
    buffer_manager: Arc<BufferManager>,
}

impl Heap {
    /// Create a new heap for a database relation.
    pub fn new(buffer_manager: Arc<BufferManager>) -> Result<Self, BufferError> {
        let frame_arc = buffer_manager.create_page()?;
        let mut frame = frame_arc.write().unwrap();

        let head_page_id = match frame.get_mut_page() {
            Some(page) => {
                RelationPage::init(page);
                RelationPage::get_id(page)
            }
            None => panic!("Head frame latch contained no page"),
        };

        buffer_manager.unpin_w(frame);

        Ok(Self {
            root_id: head_page_id,
            buffer_manager,
        })
    }

    /// Read the specified record from the relation.
    pub fn read(&self, rid: RecordId) -> Result<Record, HeapError> {
        let frame_arc = self.buffer_manager.fetch_page(rid.page_id)?;
        let frame = frame_arc.read().unwrap();

        let page = frame.get_page().unwrap();

        Ok(RelationPage::read_record(page, rid.slot_index)?)
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
        let mut page_id = self.root_id;
        loop {
            // 1) Obtain a write latch for the current page's frame.
            let frame_arc = self.buffer_manager.fetch_page(page_id)?;
            let mut frame = frame_arc.write().unwrap();

            let page = frame.get_mut_page().unwrap();

            // 2) Attempt to insert the record into the current page.
            // If the insertion was successful, return the newly initialized record ID.
            if RelationPage::insert_record(page, &mut record).is_ok() {
                frame.set_dirty_flag(true);
                self.buffer_manager.unpin_w(frame);

                return Ok(record.get_id().unwrap());
            }

            // If the insertion was unsuccessful, attempt to traverse to the next page. If there
            // is no next page, create a new page, insert the record, and link the new page to
            // the end of the heap.
            match RelationPage::get_next_page_id(page) {
                Some(pid) => {
                    self.buffer_manager.unpin_w(frame);
                    page_id = pid
                }
                None => {
                    // RELEASE write latch to current page BEFORE calling buffer manager to prevent
                    // deadlocks.
                    let prev_pid = RelationPage::get_id(page);
                    self.buffer_manager.unpin_w(frame);

                    // ACQUIRE write latch to new page, insert record, and add prev page ID.
                    let new_frame_arc = self.buffer_manager.create_page()?;
                    let mut new_frame = new_frame_arc.write().unwrap();

                    let new_page = new_frame.get_mut_page().unwrap();
                    let new_pid = RelationPage::get_id(new_page);
                    RelationPage::init(new_page);

                    RelationPage::insert_record(new_page, &mut record).unwrap();
                    RelationPage::set_prev_page_id(new_page, prev_pid);
                    new_frame.set_dirty_flag(true);

                    // RELEASE write latch to new page.
                    self.buffer_manager.unpin_w(new_frame);

                    // ACQUIRE write latch to prev page, and add next page ID.
                    let prev_frame_arc = self.buffer_manager.fetch_page(prev_pid)?;
                    let mut prev_frame = prev_frame_arc.write().unwrap();

                    let prev_page = prev_frame.get_mut_page().unwrap();

                    RelationPage::set_next_page_id(prev_page, new_pid);
                    prev_frame.set_dirty_flag(true);

                    // RELEASE write latch to prev page.
                    self.buffer_manager.unpin_w(prev_frame);

                    // Return inserted record ID.
                    return Ok(record.get_id().unwrap());
                }
            }
        }
    }

    /// Update a record in this relation and return the ID of the updated record. If the size of
    /// the new record is less than or equal to the updated record, then the ID stays the same.
    /// Otherwise, the record may be reallocated and is assigned a new record ID.
    ///
    /// Argument `record` should be an unallocated Record instance with the same schema as
    /// the record being updated. `rid` specifies the location of the record to be updated.
    pub fn update(&self, record: Record, rid: RecordId) -> Result<RecordId, HeapError> {
        if record.is_allocated() {
            return Err(HeapError::RecordAlreadyAlloc);
        }

        let frame_arc = self.buffer_manager.fetch_page(rid.page_id)?;
        let mut frame = frame_arc.write().unwrap();

        let page = frame.get_mut_page().unwrap();
        match RelationPage::update_record(page, record.clone(), rid.slot_index) {
            Ok(_) => {
                self.buffer_manager.unpin_w(frame);
                Ok(rid)
            }
            Err(e) => match e {
                PageError::PageOverflow => {
                    RelationPage::flag_delete_record(page, rid.slot_index)?;
                    RelationPage::commit_delete_record(page, rid.slot_index)?;

                    self.buffer_manager.unpin_w(frame);

                    let new_id = self.insert(record).unwrap();

                    Ok(new_id)
                }
                _ => Err(e.into()),
            },
        }
    }

    /// Flag the specified record as deleted.
    /// The record is not actually deleted until the delete operation is committed.
    pub fn flag_delete(&self, rid: RecordId) -> Result<(), HeapError> {
        let frame_arc = self.buffer_manager.fetch_page(rid.page_id)?;
        let mut frame = frame_arc.write().unwrap();

        let page = frame.get_mut_page().unwrap();
        RelationPage::flag_delete_record(page, rid.slot_index)?;

        self.buffer_manager.unpin_w(frame);

        Ok(())
    }

    /// Commit a delete operation for the specified record.
    pub fn commit_delete(&self, rid: RecordId) -> Result<(), HeapError> {
        let frame_arc = self.buffer_manager.fetch_page(rid.page_id)?;
        let mut frame = frame_arc.write().unwrap();

        let page = frame.get_mut_page().unwrap();
        RelationPage::commit_delete_record(page, rid.slot_index)?;

        self.buffer_manager.unpin_w(frame);

        Ok(())
    }

    /// Rollback a delete operation for the specified record.
    pub fn rollback_delete(&self, rid: RecordId) -> Result<(), HeapError> {
        todo!()
    }
}

/// Custom errors to be used by the heap.
#[derive(Debug, Eq, PartialEq)]
pub enum HeapError {
    /// Error to be thrown when a record to be used for insertion or replacement is already
    /// allocated elsewhere on disk.
    RecordAlreadyAlloc,

    /// Error to be thrown when a record is too large to be inserted into the database.
    /// This error should eventually become obsolete once records of arbitrary size become
    /// supported.
    RecordTooLarge,

    /// Error to be thrown when a record specified with a page ID and slot index does not exist.
    RecordDNE,

    /// Error to be thrown when a record specified with a page ID and slot index has been flagged
    /// for deletion and an operation cannot proceed.
    RecordDeleted,

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

impl From<PageError> for HeapError {
    fn from(e: PageError) -> Self {
        match e {
            PageError::PageOverflow => HeapError::RecordTooLarge,
            PageError::SlotOutOfBounds => HeapError::RecordDNE,
            PageError::RecordDeleted => HeapError::RecordDeleted,
        }
    }
}
