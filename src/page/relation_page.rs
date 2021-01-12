/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::io::{read_u32, write_u32};
use crate::common::{LsnT, PageIdT, PAGE_SIZE};
use crate::page::{Page, PageVariant};
use crate::relation::record::Record;

/// Constants for slotted-page page header
const PAGE_ID_OFFSET: u32 = 0;
const PREV_PAGE_ID_OFFSET: u32 = 4;
const NEXT_PAGE_ID_OFFSET: u32 = 8;
const FREE_POINTER_OFFSET: u32 = 12;
const NUM_RECORDS_OFFSET: u32 = 16;
const LSN_OFFSET: u32 = 20;
const RECORDS_OFFSET: u32 = 24;
const RECORD_POINTER_SIZE: u32 = 8;

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

pub struct RelationPage {
    /// A unique identifier for the page
    id: PageIdT,

    /// A copy of the raw byte array stored on disk
    data: [u8; PAGE_SIZE as usize],
}

impl Page for RelationPage {
    fn get_id(&self) -> u32 {
        self.id
    }

    fn as_bytes(&self) -> &[u8; PAGE_SIZE as usize] {
        &self.data
    }

    fn get_mut_data(&mut self) -> &mut [u8; PAGE_SIZE as usize] {
        &mut self.data
    }

    fn get_lsn(&self) -> u32 {
        read_u32(&self.data, LSN_OFFSET).unwrap()
    }

    fn set_lsn(&mut self, lsn: LsnT) {
        write_u32(&mut self.data, LSN_OFFSET, lsn).unwrap()
    }

    fn get_variant(&self) -> PageVariant {
        PageVariant::Relation
    }
}

impl RelationPage {
    /// Create a new in-memory representation of a database page.
    pub fn new(page_id: u32) -> Self {
        let mut page = Self {
            id: page_id,
            data: [0; PAGE_SIZE as usize],
        };
        page.set_page_id(page_id);
        page.set_free_space_pointer(PAGE_SIZE - 1);
        page.set_num_records(0);
        page
    }

    /// Get the page ID.
    pub fn get_page_id(&self) -> u32 {
        read_u32(&self.data, PAGE_ID_OFFSET).unwrap()
    }

    /// Set the page ID.
    pub fn set_page_id(&mut self, id: u32) {
        write_u32(&mut self.data, PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get the previous page ID.
    pub fn get_prev_page_id(&self) -> u32 {
        read_u32(&self.data, PREV_PAGE_ID_OFFSET).unwrap()
    }

    /// Set the previous page ID.
    pub fn set_prev_page_id(&mut self, id: u32) {
        write_u32(&mut self.data, PREV_PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get the next page ID.
    pub fn get_next_page_id(&self) -> u32 {
        read_u32(&self.data, NEXT_PAGE_ID_OFFSET).unwrap()
    }

    /// Set the next page ID.
    pub fn set_next_page_id(&mut self, id: u32) {
        write_u32(&mut self.data, NEXT_PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get a pointer to the next free space.
    pub fn get_free_space_pointer(&self) -> u32 {
        read_u32(&self.data, FREE_POINTER_OFFSET).unwrap()
    }

    /// Set a pointer to the next free space.
    pub fn set_free_space_pointer(&mut self, ptr: u32) {
        write_u32(&mut self.data, FREE_POINTER_OFFSET, ptr).unwrap()
    }

    /// Get the number of records contained in the page.
    pub fn get_num_records(&self) -> u32 {
        read_u32(&self.data, NUM_RECORDS_OFFSET).unwrap()
    }

    /// Set the number of records contained in the page.
    pub fn set_num_records(&mut self, num: u32) {
        write_u32(&mut self.data, NUM_RECORDS_OFFSET, num).unwrap()
    }

    /// Calculate the amount of free space (in bytes) left in the page.
    pub fn get_free_space_remaining(&self) -> u32 {
        let free_ptr = self.get_free_space_pointer();
        let num_records = self.get_num_records();
        free_ptr + 1 - RECORDS_OFFSET - num_records * RECORD_POINTER_SIZE
    }

    /// Insert a record in the page and update the header.
    pub fn insert_record(&mut self, record_data: &Vec<u8>) -> Result<(), String> {
        // Calculate header addresses for new length/offset entry
        let num_records = self.get_num_records();
        let offset_addr = RECORDS_OFFSET + num_records * RECORD_POINTER_SIZE;
        let length_addr = offset_addr + 4;

        // Bounds-check for record insertion
        let free_ptr = self.get_free_space_pointer();
        let new_free_ptr = free_ptr - record_data.len() as u32;
        if new_free_ptr < length_addr + 3 {
            return Err(format!(
                "Overflow: Record does not fit in page (ID={})",
                self.get_page_id()
            ));
        }

        // Write record data to allocated space
        let start = (new_free_ptr + 1) as usize;
        let end = (free_ptr + 1) as usize;
        for i in start..end {
            self.data[i] = record_data[i - start];
        }

        // Update header
        self.set_free_space_pointer(new_free_ptr);
        self.set_num_records(num_records + 1);
        write_u32(&mut self.data, new_free_ptr + 1, offset_addr).unwrap();
        write_u32(&mut self.data, record_data.len() as u32, length_addr).unwrap();

        Ok(())
    }

    /// Update a record in the page.
    fn update_record(&mut self, _record: Record) -> Result<(), ()> {
        Err(())
    }
}
