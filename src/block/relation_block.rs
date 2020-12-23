/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::block::{read_u32, write_u32};
use crate::common::{BlockIdT, BLOCK_SIZE};
use crate::relation::record::Record;

/// Constants for slotted-page block header
const BLOCK_ID_OFFSET: u32 = 0;
const PREV_BLOCK_ID_OFFSET: u32 = 4;
const NEXT_BLOCK_ID_OFFSET: u32 = 8;
const FREE_POINTER_OFFSET: u32 = 12;
const NUM_RECORDS_OFFSET: u32 = 16;
const LSN_OFFSET: u32 = 20;
const RECORDS_OFFSET: u32 = 24;
const RECORD_POINTER_SIZE: u32 = 8;

/// An in-memory representation of a database block with slotted-page
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
/// | BLOCK ID (4) | PREVIOUS BLOCK ID (4) | NEXT BLOCK ID (4)|
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

pub struct RelationBlock {
    /// A unique identifier for the block
    pub id: BlockIdT,
    /// A copy of the raw byte array stored on disk
    pub data: [u8; BLOCK_SIZE as usize],
    /// Number of pins on the block (pinned by concurrent threads)
    pub pin_count: u32,
    /// True if data has been modified after reading from disk
    pub is_dirty: bool,
}

impl RelationBlock {
    /// Create a new in-memory representation of a database block.
    pub fn new(block_id: u32) -> Self {
        let mut block = Self {
            id: block_id,
            data: [0; BLOCK_SIZE as usize],
            pin_count: 0,
            is_dirty: false,
        };
        block.set_block_id(block_id).unwrap();
        block.set_free_space_pointer(BLOCK_SIZE - 1).unwrap();
        block.set_num_records(0).unwrap();
        block
    }

    /// Get the block ID.
    pub fn get_block_id(&self) -> Result<u32, String> {
        read_u32(&self.data, BLOCK_ID_OFFSET)
    }

    /// Set the block ID.
    pub fn set_block_id(&mut self, id: u32) -> Result<(), String> {
        write_u32(&mut self.data, id, BLOCK_ID_OFFSET)
    }

    /// Get the previous block ID.
    pub fn get_prev_block_id(&self) -> Result<u32, String> {
        read_u32(&self.data, PREV_BLOCK_ID_OFFSET)
    }

    /// Set the previous block ID.
    pub fn set_prev_block_id(&mut self, id: u32) -> Result<(), String> {
        write_u32(&mut self.data, id, PREV_BLOCK_ID_OFFSET)
    }

    /// Get the next block ID.
    pub fn get_next_block_id(&self) -> Result<u32, String> {
        read_u32(&self.data, NEXT_BLOCK_ID_OFFSET)
    }

    /// Set the next block ID.
    pub fn set_next_block_id(&mut self, id: u32) -> Result<(), String> {
        write_u32(&mut self.data, id, NEXT_BLOCK_ID_OFFSET)
    }

    /// Get a pointer to the next free space.
    pub fn get_free_space_pointer(&self) -> Result<u32, String> {
        read_u32(&self.data, FREE_POINTER_OFFSET)
    }

    /// Set a pointer to the next free space.
    pub fn set_free_space_pointer(&mut self, ptr: u32) -> Result<(), String> {
        write_u32(&mut self.data, ptr, FREE_POINTER_OFFSET)
    }

    /// Get the number of records contained in the block.
    pub fn get_num_records(&self) -> Result<u32, String> {
        read_u32(&self.data, NUM_RECORDS_OFFSET)
    }

    /// Set the number of records contained in the block.
    pub fn set_num_records(&mut self, num: u32) -> Result<(), String> {
        write_u32(&mut self.data, num, NUM_RECORDS_OFFSET)
    }

    /// Get the log sequence number (LSN).
    pub fn get_lsn(&self) -> Result<u32, String> {
        read_u32(&self.data, LSN_OFFSET)
    }

    /// Set the log sequence number (LSN).
    pub fn set_lsn(&mut self, lsn: u32) -> Result<(), String> {
        write_u32(&mut self.data, lsn, LSN_OFFSET)
    }

    /// Calculate the amount of free space (in bytes) left in the block.
    pub fn get_free_space_remaining(&self) -> u32 {
        let free_ptr = self.get_free_space_pointer().unwrap();
        let num_records = self.get_num_records().unwrap();
        free_ptr + 1 - RECORDS_OFFSET - num_records * RECORD_POINTER_SIZE
    }

    /// Insert a record in the block and update the header.
    pub fn insert_record(&mut self, record: Record) -> Result<(), String> {
        // Calculate header addresses for new length/offset entry
        let num_records = self.get_num_records().unwrap();
        let offset_addr = RECORDS_OFFSET + num_records * RECORD_POINTER_SIZE;
        let length_addr = offset_addr + 4;

        // Bounds-check for record insertion
        let free_ptr = self.get_free_space_pointer().unwrap();
        let new_free_ptr = free_ptr - record.len();
        if new_free_ptr < length_addr + 3 {
            return Err(format!(
                "Overflow: Record does not fit in block (ID={})",
                self.get_block_id().unwrap()
            ));
        }

        // Write record data to allocated space
        let start = (new_free_ptr + 1) as usize;
        let end = (free_ptr + 1) as usize;
        for i in start..end {
            self.data[i] = record.data[i - start];
        }

        // Update header
        self.set_free_space_pointer(new_free_ptr).unwrap();
        self.set_num_records(num_records + 1).unwrap();
        write_u32(&mut self.data, new_free_ptr + 1, offset_addr).unwrap();
        write_u32(&mut self.data, record.len(), length_addr).unwrap();

        Ok(())
    }

    /// Update a record in the block.
    fn update_record(&mut self, record: Record) -> Result<(), ()> {
        Err(())
    }
}
