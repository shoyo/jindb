use super::constants::{
    BLOCK_ID_OFFSET, BLOCK_SIZE, FREE_POINTER_OFFSET, LSN_OFFSET, NEXT_BLOCK_ID_OFFSET,
    NUM_RECORDS_OFFSET, PREV_BLOCK_ID_OFFSET, RECORDS_OFFSET, RECORD_POINTER_SIZE,
};
use super::record::Record;
use crate::buffer::latch::Latch;

/// An in-memory representation of a database block with slotted-page
/// architecture. Gets written out to disk by the disk manager.
///
/// Stores a header and variable-length records that grow in opposite
/// directions, similarly to a heap and stack. Also stores information
/// to be used by the buffer manager for book-keeping such as pin
/// count and dirty flag.
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

pub struct Block {
    /// A unique identifier for the block
    pub id: u32,
    /// A copy of the raw byte array stored on disk
    pub data: [u8; BLOCK_SIZE as usize],
    /// Number of pins on the block (pinned by concurrent threads)
    pub pin_count: u32,
    /// True if data has been modified after reading from disk
    pub is_dirty: bool,
    /// Latch for concurrent access
    pub latch: Latch,
}

impl Block {
    /// Create a new in-memory representation of a database block.
    pub fn new(block_id: u32) -> Self {
        let mut block = Self {
            id: block_id,
            data: [0; BLOCK_SIZE as usize],
            pin_count: 0,
            is_dirty: false,
            latch: Latch::new(),
        };
        block.set_block_id(block_id).unwrap();
        block.set_free_space_pointer(BLOCK_SIZE - 1).unwrap();
        block.set_num_records(0).unwrap();
        block
    }

    /// Read an unsigned 32-bit integer at the specified location in the
    /// byte array.
    pub fn read_u32(&self, addr: u32) -> Result<u32, String> {
        if addr + 4 > BLOCK_SIZE {
            return Err(format!(
                "Cannot read value from byte array address (overflow)"
            ));
        }
        let addr = addr as usize;
        let mut bytes = [0; 4];
        for i in 0..4 {
            bytes[i] = self.data[addr + i];
        }
        let value = u32::from_le_bytes(bytes);
        Ok(value)
    }

    /// Write an unsigned 32-bit integer at the specified location in the
    /// byte array. The existing value is overwritten.
    pub fn write_u32(&mut self, value: u32, addr: u32) -> Result<(), String> {
        if addr + 4 > BLOCK_SIZE {
            return Err(format!(
                "Cannot write value to byte array address (overflow)"
            ));
        }
        let addr = addr as usize;
        self.data[addr] = (value & 0xff) as u8;
        self.data[addr + 1] = ((value >> 8) & 0xff) as u8;
        self.data[addr + 2] = ((value >> 16) & 0xff) as u8;
        self.data[addr + 3] = ((value >> 24) & 0xff) as u8;
        Ok(())
    }

    /// Get the block ID.
    pub fn get_block_id(&self) -> Result<u32, String> {
        self.read_u32(BLOCK_ID_OFFSET)
    }

    /// Set the block ID.
    pub fn set_block_id(&mut self, id: u32) -> Result<(), String> {
        self.write_u32(id, BLOCK_ID_OFFSET)
    }

    /// Get the previous block ID.
    pub fn get_prev_block_id(&self) -> Result<u32, String> {
        self.read_u32(PREV_BLOCK_ID_OFFSET)
    }

    /// Set the previous block ID.
    pub fn set_prev_block_id(&mut self, id: u32) -> Result<(), String> {
        self.write_u32(id, PREV_BLOCK_ID_OFFSET)
    }

    /// Get the next block ID.
    pub fn get_next_block_id(&self) -> Result<u32, String> {
        self.read_u32(NEXT_BLOCK_ID_OFFSET)
    }

    /// Set the next block ID.
    pub fn set_next_block_id(&mut self, id: u32) -> Result<(), String> {
        self.write_u32(id, NEXT_BLOCK_ID_OFFSET)
    }

    /// Get a pointer to the next free space.
    pub fn get_free_space_pointer(&self) -> Result<u32, String> {
        self.read_u32(FREE_POINTER_OFFSET)
    }

    /// Set a pointer to the next free space.
    pub fn set_free_space_pointer(&mut self, ptr: u32) -> Result<(), String> {
        self.write_u32(ptr, FREE_POINTER_OFFSET)
    }

    /// Get the number of records contained in the block.
    pub fn get_num_records(&self) -> Result<u32, String> {
        self.read_u32(NUM_RECORDS_OFFSET)
    }

    /// Set the number of records contained in the block.
    pub fn set_num_records(&mut self, num: u32) -> Result<(), String> {
        self.write_u32(num, NUM_RECORDS_OFFSET)
    }

    /// Get the log sequence number (LSN).
    pub fn get_lsn(&self) -> Result<u32, String> {
        self.read_u32(LSN_OFFSET)
    }

    /// Set the log sequence number (LSN).
    pub fn set_lsn(&mut self, lsn: u32) -> Result<(), String> {
        self.write_u32(lsn, LSN_OFFSET)
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
        self.write_u32(new_free_ptr + 1, offset_addr).unwrap();
        self.write_u32(record.len(), length_addr).unwrap();

        Ok(())
    }

    /// Update a record in the block.
    fn update_record(&mut self, record: Record) -> Result<(), ()> {
        Err(())
    }
}
