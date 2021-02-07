/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::constants::{LsnT, PageIdT, PAGE_SIZE};
use crate::io::{read_u32, write_u32};
use crate::page::{Page, PageError, PageVariant};
use crate::relation::record::{Record, RecordId};

use std::any::Any;
use std::mem::size_of;

/// Constants for slotted-page page header.
const PAGE_ID_OFFSET: u32 = 0;
const PREV_PAGE_ID_OFFSET: u32 = 4;
const NEXT_PAGE_ID_OFFSET: u32 = 8;
const FREE_POINTER_OFFSET: u32 = 12;
const NUM_RECORDS_OFFSET: u32 = 16;
const LSN_OFFSET: u32 = 20;
const RECORDS_OFFSET: u32 = 24;
const RECORD_POINTER_SIZE: u32 = 8;

/// The ID 0 is used to indicate an invalid page ID.
/// Page ID 0 will always be a metadata page reserved for the system catalog, so we don't need
/// to worry about a relation page actually having an ID equal to INVALID_PAGE_ID.
const INVALID_PAGE_ID: u32 = 0;

/// The delete mask is used to efficiently mark records in a page for deletion. The mask itself
/// is an unsigned 32-bit integer with only the leftmost bit set to 1. When a record is marked
/// for deletion, the leftmost bit of its length value in the header is set to 1 by using the
/// delete mask. To check if a record is marked for deletion, we check the leftmost bit of its
/// length.
/// This raises the question of whether or not this causes false positives. For the leftmost bit
/// of a 32-bit integer to be 1, the integer must be greater than or equal to 2147483648 (= 1 << 31)
/// which far exceeds any reasonable page size on current hardware, let alone any practical length
/// for a record in a page. (side note: I look forward to the day that claiming 2GB is too large
/// for a page record sounds silly.)
/// Although this method of record deletion is more space-efficient than allocating a boolean for
/// each record in the page, it makes working with the length value more tedious. Whenever the
/// length of a record is read from the header, we must first verify that the leftmost bit is a
/// 0 before using it to index the record on the page.
const DELETE_MASK: u32 = 1_u32 << 31;

/// An in-memory representation of a database page with slotted-page architecture. Gets written
/// out to disk by the disk manager.
///
/// Contains a header and variable-length records that grow in opposite directions, similarly to
/// a heap and stack. Also stores information to be used by the buffer manager for book-keeping
/// such as pin count and dirty flag.
///
///
/// Data format:
/// +--------------------+--------------+---------------------+
/// |  HEADER (grows ->) | ... FREE ... | (<- grows) RECORDS  |
/// +--------------------+--------------+---------------------+
///                                     ^ Free Pointer
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
///                          ^ Free Pointer

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
        let free_ptr = self.get_free_pointer() + 1;
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

    fn as_any(&self) -> &dyn Any {
        self
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
        page.set_free_pointer(PAGE_SIZE - 1);
        page.set_num_records(0);
        page
    }

    /// Set the page ID.
    pub fn set_page_id(&mut self, id: PageIdT) {
        write_u32(&mut self.bytes, PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get the previous page ID.
    pub fn get_prev_page_id(&self) -> Option<PageIdT> {
        let pid = read_u32(&self.bytes, PREV_PAGE_ID_OFFSET).unwrap();
        match pid == INVALID_PAGE_ID {
            true => None,
            false => Some(pid),
        }
    }

    /// Set the previous page ID.
    pub fn set_prev_page_id(&mut self, id: PageIdT) {
        write_u32(&mut self.bytes, PREV_PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get the next page ID.
    pub fn get_next_page_id(&self) -> Option<PageIdT> {
        let pid = read_u32(&self.bytes, NEXT_PAGE_ID_OFFSET).unwrap();
        match pid == INVALID_PAGE_ID {
            true => None,
            false => Some(pid),
        }
    }

    /// Set the next page ID.
    pub fn set_next_page_id(&mut self, id: PageIdT) {
        write_u32(&mut self.bytes, NEXT_PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get a pointer to the next free space.
    pub fn get_free_pointer(&self) -> u32 {
        read_u32(&self.bytes, FREE_POINTER_OFFSET).unwrap()
    }

    /// Set a pointer to the next free space.
    pub fn set_free_pointer(&mut self, ptr: u32) {
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

    /// Read the record at the specified slot index.
    pub fn read_record(&self, slot: u32) -> Result<Record, PageError> {
        if slot >= self.get_num_records() {
            return Err(PageError::SlotOutOfBounds);
        }
        let offset_addr = RECORDS_OFFSET + slot * RECORD_POINTER_SIZE;
        let length_addr = offset_addr + 4;

        let offset = read_u32(&self.bytes, offset_addr).unwrap() as usize;
        let length = read_u32(&self.bytes, length_addr).unwrap();

        // Check that the record has not been deleted.
        if self._is_deleted(length) {
            return Err(PageError::RecordDeleted);
        }

        let bytes = Vec::from(&self.bytes[offset..offset + length as usize]);
        let rid = RecordId {
            page_id: self.get_id(),
            slot_index: slot,
        };

        Ok(Record::from_bytes(bytes, rid))
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

        let free_ptr = self.get_free_pointer();
        let new_free_ptr = free_ptr - record.len() as u32;

        // Write record data to allocated space.
        let start = (new_free_ptr + 1) as usize;
        let end = (free_ptr + 1) as usize;
        let record_data = record.as_bytes();
        for i in start..end {
            self.bytes[i] = record_data[i - start];
        }

        // Update header.
        self.set_free_pointer(new_free_ptr);
        self.set_num_records(num_records + 1);
        write_u32(&mut self.bytes, offset_addr, new_free_ptr + 1).unwrap();
        write_u32(&mut self.bytes, length_addr, record_data.len() as u32).unwrap();

        // Update record's ID.
        record.allocate(self.get_id(), num_records);

        Ok(())
    }

    /// Update the record at the specified slot index.
    ///
    /// Argument `record` should be an unallocated Record instance with the same schema as the
    /// record being updated.
    ///
    /// NOTE: This method is currently incomplete, in that it returns an error if the new record is
    /// larger than the old record. In such a case, the caller must perform a delete -> insert
    /// instead.
    pub fn update_record(&mut self, new_record: Record, slot: u32) -> Result<(), PageError> {
        if slot >= self.get_num_records() {
            return Err(PageError::SlotOutOfBounds);
        }

        let offset_addr = RECORDS_OFFSET + slot * RECORD_POINTER_SIZE;
        let length_addr = offset_addr + 4;

        let length = read_u32(&self.bytes, length_addr).unwrap();

        // Check that the record has not been deleted.
        if self._is_deleted(length) {
            return Err(PageError::RecordDeleted);
        }

        // Check that there is enough space to insert the updated record.
        if length < new_record.len() {
            return Err(PageError::PageOverflow);
        }

        let offset = read_u32(&self.bytes, offset_addr).unwrap() as usize;

        // Update the record and header.
        for i in 0..new_record.len() as usize {
            self.bytes[offset + i] = new_record.as_bytes()[i];
        }
        write_u32(&mut self.bytes, length_addr, new_record.len()).unwrap();

        Ok(())
    }

    /// Flag the record at the specified slot index for deletion.
    /// The record is not actually deleted until the deletion is committed.
    pub fn flag_delete_record(&mut self, slot: u32) -> Result<(), PageError> {
        if slot >= self.get_num_records() {
            return Err(PageError::SlotOutOfBounds);
        }

        let length_addr = RECORDS_OFFSET + slot * RECORD_POINTER_SIZE + 4;
        let length = read_u32(&self.bytes, length_addr).unwrap();

        // Check that the record has not already been deleted.
        if self._is_deleted(length) {
            return Err(PageError::RecordDeleted);
        }

        // Flag the record for deletion.
        let new_length = self._set_delete_bit(length);
        write_u32(&mut self.bytes, length_addr, new_length).unwrap();

        Ok(())
    }

    /// Return true if the specified record is empty or flagged for deletion, false otherwise.
    fn _is_deleted(&self, record_length: u32) -> bool {
        record_length & DELETE_MASK != 0 || record_length == 0
    }

    /// Flag a given record for deletion.
    fn _set_delete_bit(&self, record_length: u32) -> u32 {
        record_length | DELETE_MASK
    }

    /// Unflag a given record for deletion.
    fn _unset_delete_bit(&self, record_length: u32) -> u32 {
        record_length & !DELETE_MASK
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{read_bool, read_f32, read_i32, read_str};
    use crate::relation::record::NULL_BITMAP_SIZE;
    use crate::relation::types::{size_of, DataType};
    use crate::relation::Attribute;
    use crate::relation::Schema;
    use std::sync::Arc;

    #[test]
    fn test_insert_record() {
        // Initialize empty page.
        let mut page = RelationPage::new(10);
        assert_eq!(page.get_id(), 10);
        assert!(page.get_next_page_id().is_none());
        assert!(page.get_prev_page_id().is_none());
        assert_eq!(page.get_num_records(), 0);
        assert_eq!(page.get_free_space(), PAGE_SIZE - RECORDS_OFFSET);

        let varchar = "Hello, World!".to_string();
        let varchar_len = varchar.len() as u32;

        // Initialize record to be inserted.
        let mut record = Record::new(
            vec![
                Some(Box::new(varchar)),
                Some(Box::new(true)),
                Some(Box::new(123_456_i32)),
                Some(Box::new(std::f32::consts::PI)),
            ],
            Arc::new(Schema::new(vec![
                Attribute::new("varch", DataType::Varchar, false, false, false),
                Attribute::new("bool", DataType::Boolean, false, false, false),
                Attribute::new("int", DataType::Int, false, false, false),
                Attribute::new("deci", DataType::Decimal, false, false, false),
            ])),
        )
        .unwrap();

        // Insert record into page.
        page.insert_record(&mut record).unwrap();
        assert_eq!(page.get_num_records(), 1);
        assert_eq!(
            page.get_free_space(),
            PAGE_SIZE - RECORDS_OFFSET - record.len() - RECORD_POINTER_SIZE
        );
        assert_eq!(page.get_free_pointer(), PAGE_SIZE - record.len() - 1);

        // Assert that record bytes were written to the correct locations in the page.

        // Expected page layout:
        // +-------------------------------------------------------------------------+
        // |  PAGE  | RECORD | RECORD | ... | RECORD | RECORD FIXED- |  RECORD VAR-  |
        // | HEADER | OFFSET | LENGTH | ... | BITMAP | LENGTH VALUES | LENGTH VALUES |
        // +-------------------------------------------------------------------------+
        // ^0       ^ RECORDS_OFFSET        ^ FREE POINTER               PAGE_SIZE-1 ^
        //                                  |____________ record.len() ______________|

        let page_bytes = page.as_bytes();

        let offset_addr = RECORDS_OFFSET;
        let length_addr = RECORDS_OFFSET + 4;
        assert_eq!(
            read_u32(page_bytes, offset_addr).unwrap(),
            PAGE_SIZE - record.len()
        );
        assert_eq!(read_u32(page_bytes, length_addr).unwrap(), record.len());

        let bitmap_size = NULL_BITMAP_SIZE;
        let bitmap_addr = PAGE_SIZE - record.len();
        let str_offset_addr = bitmap_addr + bitmap_size;
        let str_length_addr = str_offset_addr + 4;
        let bool_addr = str_length_addr + 4;
        let int_addr = bool_addr + size_of(DataType::Boolean);
        let deci_addr = int_addr + size_of(DataType::Int);
        let str_val_addr = deci_addr + size_of(DataType::Decimal);

        assert_eq!(read_u32(page_bytes, bitmap_addr).unwrap(), 0);
        assert_eq!(
            read_u32(page_bytes, str_offset_addr).unwrap(),
            record.len() - varchar_len
        );
        assert_eq!(read_u32(page_bytes, str_length_addr).unwrap(), varchar_len);
        assert_eq!(read_bool(page_bytes, bool_addr).unwrap(), true);
        assert_eq!(read_i32(page_bytes, int_addr).unwrap(), 123_456_i32);
        assert_eq!(
            read_f32(page_bytes, deci_addr).unwrap(),
            std::f32::consts::PI
        );
        assert_eq!(
            read_str(page_bytes, str_val_addr, varchar_len).unwrap(),
            "Hello, World!".to_string()
        );
    }
}
