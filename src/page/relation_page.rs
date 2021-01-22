/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::io::{read_u32, write_u32};
use crate::common::{LsnT, PAGE_SIZE};
use crate::page::{Page, PageError, PageVariant};
use crate::relation::record::Record;

use std::any::Any;

/// Constants for slotted-page page header.
const PAGE_ID_OFFSET: u32 = 0;
const PREV_PAGE_ID_OFFSET: u32 = 4;
const NEXT_PAGE_ID_OFFSET: u32 = 8;
const FREE_POINTER_OFFSET: u32 = 12;
const NUM_RECORDS_OFFSET: u32 = 16;
const LSN_OFFSET: u32 = 20;
const RECORDS_OFFSET: u32 = 24;
const RECORD_POINTER_SIZE: u32 = 8;
const UNINITIALIZED: u32 = 0;

/// An in-memory representation of a database page with slotted-page
/// architecture. Gets written out to disk by the disk manager.
///
/// Contains a header and variable-length records that grow in opposite
/// directions, similarly to a heap and stack. Also stores information
/// to be used by the buffer manager for book-keeping such as pin count
/// and dirty flag.
///
///
/// Data format:
/// +--------------------+--------------+---------------------+
/// |  HEADER (grows ->) | ... FREE ... | (<- grows) RECORDS  |
/// +--------------------+--------------+---------------------+
///                                     ^ Free Space Pointer
///
///
/// Header metadata (number denotes size in bytes):
/// +--------------+-----------------------+------------------+
/// |  PAGE ID (4) |  PREVIOUS PAGE ID (4) | NEXT PAGE ID (4) |
/// +--------------+-----------------------+------------------+
/// +------------------------+-----------------+--------------+
/// | FREE SPACE POINTER (4) | NUM RECORDS (4) |    LSN (4)   |
/// +------------------------+-----------------+--------------+
/// +---------------------+---------------------+-------------+
/// | RECORD 1 OFFSET (4) | RECORD 1 LENGTH (4) |     ...     |
/// +---------------------+---------------------+-------------+
///
///
/// Records:
/// +------------------------+----------+----------+----------+
/// |           ...          | RECORD 3 | RECORD 2 | RECORD 1 |
/// +------------------------+----------+----------+----------+
///                          ^ Free Space Pointer

pub struct RelationPage {
    bytes: [u8; PAGE_SIZE as usize],
}

impl Page for RelationPage {
    fn get_id(&self) -> u32 {
        read_u32(&self.bytes, PAGE_ID_OFFSET).unwrap()
    }

    fn as_bytes(&self) -> &[u8; PAGE_SIZE as usize] {
        &self.bytes
    }

    fn as_mut_bytes(&mut self) -> &mut [u8; PAGE_SIZE as usize] {
        &mut self.bytes
    }

    fn get_lsn(&self) -> u32 {
        read_u32(&self.bytes, LSN_OFFSET).unwrap()
    }

    fn set_lsn(&mut self, lsn: LsnT) {
        write_u32(&mut self.bytes, LSN_OFFSET, lsn).unwrap()
    }

    fn get_free_space(&self) -> u32 {
        let free_ptr = self.get_free_space_pointer() + 1;
        let num_records = self.get_num_records();

        let header = RECORDS_OFFSET + num_records * RECORD_POINTER_SIZE;
        match header >= free_ptr {
            true => 0,
            false => free_ptr - header,
        }
    }

    fn get_variant(&self) -> PageVariant {
        PageVariant::Relation
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

impl RelationPage {
    /// Create a new in-memory representation of a database page.
    pub fn new(page_id: u32) -> Self {
        let mut page = Self {
            bytes: [0; PAGE_SIZE as usize],
        };
        page.set_page_id(page_id);
        page.set_free_space_pointer(PAGE_SIZE - 1);
        page.set_num_records(0);
        page
    }

    /// Set the page ID.
    pub fn set_page_id(&mut self, id: u32) {
        write_u32(&mut self.bytes, PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get the previous page ID.
    pub fn get_prev_page_id(&self) -> u32 {
        read_u32(&self.bytes, PREV_PAGE_ID_OFFSET).unwrap()
    }

    /// Set the previous page ID.
    pub fn set_prev_page_id(&mut self, id: u32) {
        write_u32(&mut self.bytes, PREV_PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get the next page ID.
    pub fn get_next_page_id(&self) -> Option<u32> {
        let pid = read_u32(&self.bytes, NEXT_PAGE_ID_OFFSET).unwrap();
        match pid == UNINITIALIZED {
            true => None,
            false => Some(pid),
        }
    }

    /// Set the next page ID.
    pub fn set_next_page_id(&mut self, id: u32) {
        write_u32(&mut self.bytes, NEXT_PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get a pointer to the next free space.
    pub fn get_free_space_pointer(&self) -> u32 {
        read_u32(&self.bytes, FREE_POINTER_OFFSET).unwrap()
    }

    /// Set a pointer to the next free space.
    pub fn set_free_space_pointer(&mut self, ptr: u32) {
        write_u32(&mut self.bytes, FREE_POINTER_OFFSET, ptr).unwrap()
    }

    /// Get the number of records contained in the page.
    pub fn get_num_records(&self) -> u32 {
        read_u32(&self.bytes, NUM_RECORDS_OFFSET).unwrap()
    }

    /// Set the number of records contained in the page.
    pub fn set_num_records(&mut self, num: u32) {
        write_u32(&mut self.bytes, NUM_RECORDS_OFFSET, num).unwrap()
    }

    /// Insert a record in the page and update the header.
    pub fn insert_record(&mut self, record: &mut Record) -> Result<(), PageError> {
        // Bounds-check for record insertion.
        if record.len() + RECORD_POINTER_SIZE > self.get_free_space() {
            return Err(PageError::PageOverflow);
        }

        // Calculate header addresses for new length/offset entry.
        let num_records = self.get_num_records();
        let offset_addr = RECORDS_OFFSET + num_records * RECORD_POINTER_SIZE;
        let length_addr = offset_addr + 4;

        let free_ptr = self.get_free_space_pointer();
        let new_free_ptr = free_ptr - record.len() as u32;

        // Write record data to allocated space.
        let start = (new_free_ptr + 1) as usize;
        let end = (free_ptr + 1) as usize;
        let record_data = record.as_bytes();
        for i in start..end {
            self.bytes[i] = record_data[i - start];
        }

        // Update header.
        self.set_free_space_pointer(new_free_ptr);
        self.set_num_records(num_records + 1);
        write_u32(&mut self.bytes, new_free_ptr + 1, offset_addr).unwrap();
        write_u32(&mut self.bytes, record_data.len() as u32, length_addr).unwrap();

        // Update record's ID.
        record.allocate(self.get_id(), num_records);

        Ok(())
    }

    /// Update a record in the page.
    fn update_record(&mut self, _record: Record) -> Result<(), ()> {
        Err(())
    }
}
