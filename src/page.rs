/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jindb for more information about this project and its license.
 */

use crate::constants::{LsnT, PageIdT, PAGE_SIZE};
use crate::io::{read_u32, write_u32};
use crate::relation::record::{Record, RecordId};

/// Type alias for a byte array that represents an arbitrary page on disk.
pub type PageBytes = [u8; PAGE_SIZE as usize];

/// Type alias for the page ID in a page byte array.
/// Each page type has a different byte layout, but offset 0 is always reserved for the page ID.
const PAGE_ID_OFFSET: u32 = 0;

/// ===== RAW PAGE =====

/// Utility struct for handling page byte arrays in low layers of the database.
pub struct RawPage;

impl RawPage {
    pub fn new(id: PageIdT) -> PageBytes {
        let mut page = [0; PAGE_SIZE as usize];
        RawPage::set_id(&mut page, id);
        page
    }

    pub fn get_id(bytes: &PageBytes) -> PageIdT {
        read_u32(bytes, PAGE_ID_OFFSET).unwrap()
    }

    pub fn set_id(bytes: &mut PageBytes, id: PageIdT) {
        write_u32(bytes, PAGE_ID_OFFSET, id).unwrap();
    }
}

/// ===== RELATION PAGE =====

/// Constants for slotted-page page header in relation pages.
const PREV_PAGE_ID_OFFSET: u32 = 4;
const NEXT_PAGE_ID_OFFSET: u32 = 8;
const FREE_POINTER_OFFSET: u32 = 12;
const NUM_RECORDS_OFFSET: u32 = 16;
const LSN_OFFSET: u32 = 20;
const RECORDS_OFFSET: u32 = 24;
const RECORD_POINTER_SIZE: u32 = 8;

/// Type aliases for readability.
type RecordOffsetT = u32;
type RecordSizeT = u32;

/// The ID 0 is used to indicate an invalid page ID.
/// Page ID 0 will always be a metadata page reserved for the system catalog, so we don't need
/// to worry about a relation page actually having an ID equal to INVALID_PAGE_ID.
const INVALID_PAGE_ID: u32 = 0;

/// The delete mask is used to efficiently mark records in a page for deletion. The mask itself
/// is an unsigned 32-bit integer with only the leftmost bit set to 1. When a record is marked
/// for deletion, the leftmost bit of its size value in the header is set to 1 by using the
/// delete mask. To check if a record is marked for deletion, we check the leftmost bit of its
/// size.
/// This raises the question of whether or not this causes false positives. For the leftmost bit
/// of a 32-bit integer to be 1, the integer must be greater than or equal to 2147483648 (= 1 << 31)
/// which far exceeds any reasonable page size on current hardware, let alone any practical size
/// for a record in a page. (side note: I look forward to the day that claiming 2GB is too large
/// for a page record sounds silly.)
/// Although this method of record deletion is more space-efficient than allocating a boolean for
/// each record in the page, it makes working with the size value more tedious. Whenever the
/// size of a record is read from the header, we must first verify that the leftmost bit is a
/// 0 before using it to index the record on the page.
const DELETE_MASK: u32 = 1_u32 << 31;

/// An in-memory representation of a database page with slotted-page architecture.
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
/// +---------------------+-------------------+---------------+
/// | RECORD 1 OFFSET (4) | RECORD 1 SIZE (4) |      ...      |
/// +---------------------+-------------------+---------------+
///
///
/// Records:
/// +------------------------+----------+----------+----------+
/// |           ...          | RECORD 3 | RECORD 2 | RECORD 1 |
/// +------------------------+----------+----------+----------+
///                          ^ Free Pointer
pub struct RelationPage;

impl RelationPage {
    /// Initialize a relation page.
    /// Assumes that `bytes` is a newly initialized page byte array with its page ID set.
    pub fn init(bytes: &mut PageBytes) {
        RelationPage::set_free_pointer(bytes, PAGE_SIZE - 1);
    }

    /// Get the page ID.
    pub fn get_id(bytes: &PageBytes) -> PageIdT {
        read_u32(bytes, PAGE_ID_OFFSET).unwrap()
    }

    /// Set the page ID.
    pub fn set_id(bytes: &mut PageBytes, id: PageIdT) {
        write_u32(bytes, PAGE_ID_OFFSET, id).unwrap();
    }

    /// Get the previous page ID.
    pub fn get_prev_page_id(bytes: &PageBytes) -> Option<PageIdT> {
        let pid = read_u32(bytes, PREV_PAGE_ID_OFFSET).unwrap();
        match pid == INVALID_PAGE_ID {
            true => None,
            false => Some(pid),
        }
    }

    /// Set the previous page ID.
    pub fn set_prev_page_id(bytes: &mut PageBytes, id: PageIdT) {
        write_u32(bytes, PREV_PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get the next page ID.
    pub fn get_next_page_id(bytes: &PageBytes) -> Option<PageIdT> {
        let pid = read_u32(bytes, NEXT_PAGE_ID_OFFSET).unwrap();
        match pid == INVALID_PAGE_ID {
            true => None,
            false => Some(pid),
        }
    }

    /// Set the next page ID.
    pub fn set_next_page_id(bytes: &mut PageBytes, id: PageIdT) {
        write_u32(bytes, NEXT_PAGE_ID_OFFSET, id).unwrap()
    }

    /// Get a pointer to the next free space.
    pub fn get_free_pointer(bytes: &PageBytes) -> u32 {
        read_u32(bytes, FREE_POINTER_OFFSET).unwrap()
    }

    /// Set a pointer to the next free space.
    pub fn set_free_pointer(bytes: &mut PageBytes, ptr: u32) {
        write_u32(bytes, FREE_POINTER_OFFSET, ptr).unwrap()
    }

    /// Get the number of records contained in the page.
    pub fn get_num_records(bytes: &PageBytes) -> u32 {
        read_u32(bytes, NUM_RECORDS_OFFSET).unwrap()
    }

    /// Set the number of records contained in the page.
    pub fn set_num_records(bytes: &mut PageBytes, num: u32) {
        write_u32(bytes, NUM_RECORDS_OFFSET, num).unwrap()
    }

    /// Get the log sequence number of the page.
    fn get_lsn(bytes: &PageBytes) -> u32 {
        read_u32(bytes, LSN_OFFSET).unwrap()
    }

    /// Set the log sequence number of the page.
    fn set_lsn(bytes: &mut PageBytes, lsn: LsnT) {
        write_u32(bytes, LSN_OFFSET, lsn).unwrap()
    }

    /// Return the amount of free space left in the page in bytes.
    fn get_free_space(bytes: &PageBytes) -> u32 {
        let free_ptr = RelationPage::get_free_pointer(bytes) + 1;
        let num_records = RelationPage::get_num_records(bytes);

        let header = RECORDS_OFFSET + num_records * RECORD_POINTER_SIZE;
        match header >= free_ptr {
            true => 0,
            false => free_ptr - header,
        }
    }

    /// Read the record at the specified slot index.
    pub fn read_record(bytes: &PageBytes, slot: u32) -> Result<Record, PageError> {
        let (offset_addr, size_addr) = RelationPage::get_ptr_addrs(bytes, slot)?;
        let offset = read_u32(bytes, offset_addr).unwrap() as usize;
        let size = read_u32(bytes, size_addr).unwrap();

        // Check that the record has not been deleted.
        if RelationPage::is_deleted(size) {
            return Err(PageError::RecordDeleted);
        }

        let record_bytes = Vec::from(&bytes[offset..offset + size as usize]);
        let rid = RecordId {
            page_id: RelationPage::get_id(bytes),
            slot_index: slot,
        };

        Ok(Record::from_bytes(record_bytes, rid))
    }

    /// Insert a record in the page and update the header.
    pub fn insert_record(bytes: &mut PageBytes, record: &mut Record) -> Result<(), PageError> {
        // Bounds-check for record insertion.
        if record.len() + RECORD_POINTER_SIZE > RelationPage::get_free_space(bytes) {
            return Err(PageError::PageOverflow);
        }

        // Calculate header addresses for new size/offset entry.
        let num_records = RelationPage::get_num_records(bytes);
        let offset_addr = RECORDS_OFFSET + num_records * RECORD_POINTER_SIZE;
        let size_addr = offset_addr + 4;

        let free_ptr = RelationPage::get_free_pointer(bytes);
        let new_free_ptr = free_ptr - record.len() as u32;

        // Write record data to allocated space.
        let start = (new_free_ptr + 1) as usize;
        let end = (free_ptr + 1) as usize;
        let record_data = record.as_bytes();
        for i in start..end {
            bytes[i] = record_data[i - start];
        }

        // Update header.
        RelationPage::set_free_pointer(bytes, new_free_ptr);
        RelationPage::set_num_records(bytes, num_records + 1);
        write_u32(bytes, offset_addr, new_free_ptr + 1).unwrap();
        write_u32(bytes, size_addr, record_data.len() as u32).unwrap();

        // Update record's ID.
        record.allocate(RelationPage::get_id(bytes), num_records);

        Ok(())
    }

    /// Update the record at the specified slot index. If the page does not have enough space to
    /// update the record (i.e. the new record is larger than the older value and the page is
    /// full), then return an error. The caller must perform a delete-then-insert instead.
    ///
    /// The argument `record` should be an unallocated Record instance with the same schema as the
    /// record being updated.
    ///
    /// Implementation:
    /// In the case that the update fits on the current page, we shift over every byte between
    /// the free pointer and record to be updated by the size difference between the new and old
    /// record. We write the new record into the newly adjusted space. If the new record is
    /// smaller than the old record, we shift the bytes to the right, and vice versa.
    ///
    /// Afterward, we need to update the pointer of records to the left of the updated record.
    /// Therefore, all records with an offset LESS than the old record's offset (with a non-zero
    /// size entry) have their offset adjusted by the size difference between the new and old
    /// record.
    ///
    /// Before update:
    /// +------------------------------------------------------------------------+
    /// | Header |        ...        | records | RECORD TO UPDATE | more records |
    /// +------------------------------------------------------------------------+
    ///                 Free pointer ^         ^ offset
    ///                                        |------ size ------|
    ///
    /// After update (to larger record):
    /// +------------------------------------------------------------------------+
    /// | Header |     ...     | records |     UPDATED RECORD     | more records |
    /// +------------------------------------------------------------------------+
    ///           Free pointer ^         ^ offset
    ///                                  |-----|------ size ------|
    ///                 size difference (+) ^
    ///
    /// After update (to smaller record):
    /// +------------------------------------------------------------------------+
    /// | Header |         ...               | records |  RECORD  | more records |
    /// +------------------------------------------------------------------------+
    ///                         Free pointer ^         ^ offset
    ///                                        |-------|---size---|
    ///                         size difference (-) ^
    ///
    pub fn update_record(
        bytes: &mut PageBytes,
        new_record: Record,
        slot: u32,
    ) -> Result<(), PageError> {
        let (offset_addr, size_addr) = RelationPage::get_ptr_addrs(bytes, slot)?;
        let offset = read_u32(bytes, offset_addr).unwrap() as usize;
        let old_size = read_u32(bytes, size_addr).unwrap();
        let new_size = new_record.size();

        // Check that the record has not been deleted.
        if RelationPage::is_deleted(old_size) {
            return Err(PageError::RecordDeleted);
        }

        // Check that there is enough space to insert the updated record.
        // If there is not enough space, then the caller must delete-then-insert instead.
        if RelationPage::get_free_space(bytes) + old_size < new_size {
            return Err(PageError::PageOverflow);
        }

        // Shift over bytes using a temporary buffer.
        let free_ptr = RelationPage::get_free_pointer(bytes);

        let src = free_ptr as usize;
        let dst = (free_ptr + old_size - new_size) as usize;
        let cnt = offset - free_ptr as usize;

        let mut buf = vec![0; cnt];
        for i in 0..cnt {
            buf[i] = bytes[src + i];
        }
        for i in 0..cnt {
            bytes[dst + i] = buf[i];
        }

        // Write update to newly adjusted space.
        let new_offset = (offset as u32 + old_size - new_size) as usize;
        let new_bytes = new_record.as_bytes();
        for i in 0..new_size as usize {
            bytes[new_offset + i] = new_bytes[i];
        }

        // Update header.
        RelationPage::set_free_pointer(bytes, dst as u32);
        write_u32(bytes, size_addr, new_size).unwrap();

        for slot_idx in 0..RelationPage::get_num_records(bytes) {
            let (offset_addr, size_addr) = RelationPage::get_ptr_addrs(bytes, slot_idx).unwrap();
            let t_offset = read_u32(bytes, offset_addr).unwrap();
            let t_size = read_u32(bytes, size_addr).unwrap();

            if t_offset < offset as u32 + old_size && t_size > 0 {
                let new_t_offset = t_offset + old_size - new_size;
                write_u32(bytes, offset_addr, new_t_offset).unwrap();
            }
        }

        Ok(())
    }

    /// Flag the record at the specified slot index for deletion.
    /// The record is not actually deleted until the deletion is committed.
    pub fn flag_delete_record(bytes: &mut PageBytes, slot: u32) -> Result<(), PageError> {
        let (_, size_addr) = RelationPage::get_ptr_addrs(bytes, slot)?;

        let size = read_u32(bytes, size_addr).unwrap();

        // Check that the record has not already been deleted.
        if RelationPage::is_deleted(size) {
            return Err(PageError::RecordDeleted);
        }

        // Flag the record for deletion.
        let new_size = RelationPage::set_delete_bit(size);
        write_u32(bytes, size_addr, new_size).unwrap();

        Ok(())
    }

    /// Delete the record at the specified slot index.
    /// If the record has been flagged for deletion, then we are committing the deletion and
    /// actually removing the record from the page.
    /// If the record has NOT been flagged for deletion, then we are rolling back an insertion.
    ///
    /// Implementation:
    /// We shift over every byte between the free pointer and the record to be deleted to the
    /// right, by the size of the deleted record.
    ///
    /// After deletion, we need to update the pointers of records to the left of the deleted
    /// record. Therefore, all records with an offset LESS than the deleted record (with a
    /// non-zero size entry) have their offset INCREASED by the size of the deleted record.
    ///
    /// Before deletion:
    /// +--------------------------------------------------------------+
    /// | Header |   ...   | records | RECORD TO DELETE | more records |
    /// +--------------------------------------------------------------+
    ///       Free pointer ^         ^ offset
    ///                              |------ size ------|
    ///
    /// After deletion:
    /// +--------------------------------------------------------------+
    /// | Header |            ...             | records | more records |
    /// +--------------------------------------------------------------+
    ///                          Free pointer ^
    ///
    pub fn commit_delete_record(bytes: &mut PageBytes, slot: u32) -> Result<(), PageError> {
        let (offset_addr, size_addr) = RelationPage::get_ptr_addrs(bytes, slot)?;
        let offset = read_u32(bytes, offset_addr).unwrap();
        let mut size = read_u32(bytes, size_addr).unwrap();

        // If the record is flagged for deletion, we obtain the correct record size before
        // proceeding.
        if RelationPage::is_deleted(size) {
            size = RelationPage::unset_delete_bit(size);
        }

        // Shift over bytes using a temporary buffer.
        let free_ptr = RelationPage::get_free_pointer(bytes);

        let src = free_ptr as usize;
        let dst = (free_ptr + size) as usize;
        let cnt = (offset - free_ptr) as usize;

        let mut buf = vec![0; cnt];
        for i in 0..cnt {
            buf[i] = bytes[src + i];
        }
        for i in 0..cnt {
            bytes[dst + i] = buf[i];
        }

        // Update header.
        RelationPage::set_free_pointer(bytes, dst as u32);
        write_u32(bytes, offset_addr, 0).unwrap();
        write_u32(bytes, size_addr, 0).unwrap();

        for slot_idx in 0..RelationPage::get_num_records(bytes) {
            let (offset_addr, size_addr) = RelationPage::get_ptr_addrs(bytes, slot_idx).unwrap();
            let t_offset = read_u32(bytes, offset_addr).unwrap();
            let t_size = read_u32(bytes, size_addr).unwrap();

            if t_offset < offset && t_size != 0 {
                let new_t_offset = t_offset + size;
                write_u32(bytes, offset_addr, new_t_offset).unwrap();
            }
        }

        Ok(())
    }

    /// Return true if the specified record is empty or flagged for deletion, false otherwise.
    fn is_deleted(record_size: u32) -> bool {
        record_size & DELETE_MASK != 0 || record_size == 0
    }

    /// Flag a given record for deletion.
    fn set_delete_bit(record_size: u32) -> u32 {
        record_size | DELETE_MASK
    }

    /// Unflag a given record for deletion.
    fn unset_delete_bit(record_size: u32) -> u32 {
        record_size & !DELETE_MASK
    }

    /// Return the byte array addresses of the offset and size at a given slot index.
    /// Return an error if the slot index is out of bounds.
    #[inline]
    fn get_ptr_addrs(
        bytes: &PageBytes,
        slot: u32,
    ) -> Result<(RecordOffsetT, RecordSizeT), PageError> {
        if slot >= RelationPage::get_num_records(bytes) {
            return Err(PageError::SlotOutOfBounds);
        }

        let offset_addr = RECORDS_OFFSET + slot * RECORD_POINTER_SIZE;
        let size_addr = offset_addr + 4;

        Ok((offset_addr, size_addr))
    }
}

/// ===== INDEX PAGE =====

/// An in-memory representation of a database page storing an index. The index contains key-value
/// pairs of column values to record IDs.
///
/// Data format (number denotes size in bytes):
/// +-------------+-----------------+--------------+---------------+-----------+
/// | PAGE ID (4) | ENTRY COUNT (4) | COLUMN 1 (?) | RECORD ID (8) |    ...    |
/// +-------------+-----------------+--------------+---------------+-----------+
///
/// Note that record ID is represented internally as the page ID (4 bytes) and slot index (4
/// bytes), which sums to 8 bytes.
pub struct IndexPage;

impl IndexPage {}

/// Custom errors to be used by pages.
#[derive(Debug)]
pub enum PageError {
    /// Error to be thrown when a page insertion/update would trigger an overflow.
    PageOverflow,

    /// Error to be thrown when a slot index is out of bounds.
    SlotOutOfBounds,

    /// Error to be thrown when a specified record has already been deleted and a
    /// read/update/delete operation cannot proceed.
    RecordDeleted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{read_bool, read_f32, read_i32, read_str, read_u32};
    use crate::relation::record::NULL_BITMAP_SIZE;
    use crate::relation::types::{size_of, DataType};
    use crate::relation::Attribute;
    use crate::relation::Schema;
    use std::sync::Arc;

    #[test]
    fn test_insert_record() {
        // Initialize empty page.
        let mut page = RawPage::new(5);
        RelationPage::init(&mut page);
        assert_eq!(RelationPage::get_id(&page), 5);
        assert!(RelationPage::get_next_page_id(&page).is_none());
        assert!(RelationPage::get_prev_page_id(&page).is_none());
        assert_eq!(RelationPage::get_num_records(&page), 0);
        assert_eq!(
            RelationPage::get_free_space(&page),
            PAGE_SIZE - RECORDS_OFFSET
        );

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
        RelationPage::insert_record(&mut page, &mut record).unwrap();
        assert_eq!(RelationPage::get_num_records(&page), 1);
        assert_eq!(
            RelationPage::get_free_space(&page),
            PAGE_SIZE - RECORDS_OFFSET - record.len() - RECORD_POINTER_SIZE
        );
        assert_eq!(
            RelationPage::get_free_pointer(&page),
            PAGE_SIZE - record.len() - 1
        );

        // Assert that record bytes were written to the correct locations in the page.

        // Expected page layout:
        // +-------------------------------------------------------------------------+
        // |  PAGE  | RECORD | RECORD | ... | RECORD | RECORD FIXED- |  RECORD VAR-  |
        // | HEADER | OFFSET |  SIZE  | ... | BITMAP | SIZE VALUES   |  SIZE VALUES  |
        // +-------------------------------------------------------------------------+
        // ^ 0      ^ RECORDS_OFFSET        ^ FREE POINTER               PAGE_SIZE-1 ^
        //                                  |____________ record.len() ______________|

        let offset_addr = RECORDS_OFFSET;
        let size_addr = RECORDS_OFFSET + 4;
        assert_eq!(
            read_u32(&page, offset_addr).unwrap(),
            PAGE_SIZE - record.len()
        );
        assert_eq!(read_u32(&page, size_addr).unwrap(), record.len());

        let bitmap_size = NULL_BITMAP_SIZE;
        let bitmap_addr = PAGE_SIZE - record.len();
        let str_offset_addr = bitmap_addr + bitmap_size;
        let str_size_addr = str_offset_addr + 4;
        let bool_addr = str_size_addr + 4;
        let int_addr = bool_addr + size_of(DataType::Boolean);
        let deci_addr = int_addr + size_of(DataType::Int);
        let str_val_addr = deci_addr + size_of(DataType::Decimal);

        assert_eq!(read_u32(&page, bitmap_addr).unwrap(), 0);
        assert_eq!(
            read_u32(&page, str_offset_addr).unwrap(),
            record.len() - varchar_len
        );
        assert_eq!(read_u32(&page, str_size_addr).unwrap(), varchar_len);
        assert_eq!(read_bool(&page, bool_addr).unwrap(), true);
        assert_eq!(read_i32(&page, int_addr).unwrap(), 123_456_i32);
        assert_eq!(read_f32(&page, deci_addr).unwrap(), std::f32::consts::PI);
        assert_eq!(
            read_str(&page, str_val_addr, varchar_len).unwrap(),
            "Hello, World!".to_string()
        );
    }
}
