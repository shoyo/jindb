use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;

const DB_FILENAME: &str = "db.minusql";
const BLOCK_SIZE: u32 = 64;

/// The disk manager is responsible for managing blocks stored on disk.

pub struct DiskManager;

impl DiskManager {
    pub fn new() -> Self {
        Self
    }

    pub fn write_block(&self, block_id: u32, block: Block) -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(DB_FILENAME)?;

        let offset = block_id * BLOCK_SIZE;
        file.seek(SeekFrom::Start(offset as u64))?;
        file.write_all(&block.data);
        file.flush()?;

        Ok(())
    }

    pub fn read_block(&self, block_id: u32) -> std::io::Result<[u8; BLOCK_SIZE as usize]> {
        let mut file = File::open(DB_FILENAME)?;

        let offset = block_id * BLOCK_SIZE;
        file.seek(SeekFrom::Start(offset as u64))?;
        let mut buf = [0; BLOCK_SIZE as usize];
        file.read_exact(&mut buf)?;

        Ok(buf)
    }
}

const BLOCK_ID_OFFSET: u32 = 0;
const FREE_POINTER_OFFSET: u32 = 4;
const NUM_RECORDS_OFFSET: u32 = 8;
const RECORDS_OFFSET: u32 = 12;
const RECORD_POINTER_SIZE: u32 = 8;

/// A database block with slotted-page architecture.
/// Stores a header and variable-length records that grow in opposite
/// directions, similarly to a heap and stack.
///
/// Data format:
/// ---------------------------------------------------------
///   HEADER (grows ->) | ... FREE ... | (<- grows) RECORDS
/// ---------------------------------------------------------
///                                    ^ Free Space Pointer
///
///
/// Header metadata (number denotes size in bytes):
/// ---------------------------------------------------------
///  BLOCK ID (4) | FREE SPACE POINTER (4) | NUM RECORDS (4)
/// ---------------------------------------------------------
/// ---------------------------------------------------------
///  RECORD 1 OFFSET (4) | RECORD 1 LENGTH (4) |     ...
/// ---------------------------------------------------------
///
///
/// Records:
/// ---------------------------------------------------------
///            ...          | RECORD 3 | RECORD 2 | RECORD 1
/// ---------------------------------------------------------

pub struct Block {
    data: [u8; BLOCK_SIZE as usize],
}

impl Block {
    pub fn new(block_id: u32) -> Self {
        let mut block = Self {
            data: [0; BLOCK_SIZE as usize],
        };
        block.set_block_id(block_id);
        block.set_free_space_pointer(BLOCK_SIZE - 1);
        block.set_num_records(0);
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

    /// Get a pointer to the next free space.
    pub fn get_free_space_pointer(&self) -> Result<u32, String> {
        self.read_u32(FREE_POINTER_OFFSET)
    }

    /// Set a pointer to the next free space.
    pub fn set_free_space_pointer(&mut self, ptr: u32) -> Result<(), String> {
        self.write_u32(ptr, FREE_POINTER_OFFSET)
    }

    /// Get the numer of records contained in the block.
    pub fn get_num_records(&self) -> Result<u32, String> {
        self.read_u32(NUM_RECORDS_OFFSET)
    }

    /// Set the number of records contained in the block.
    pub fn set_num_records(&mut self, num: u32) -> Result<(), String> {
        self.write_u32(num, NUM_RECORDS_OFFSET)
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
        self.set_free_space_pointer(new_free_ptr);
        self.set_num_records(num_records + 1);
        self.write_u32(new_free_ptr + 1, offset_addr);
        self.write_u32(record.len(), length_addr);

        Ok(())
    }

    /// Update a record in the block.
    fn update_record(&mut self, record: Record) {}
}

/// A serialized representation of a database block meant for
/// testing and debugging.

struct BlockRepresentation {
    block_id: u32,
    free_space_pointer: u32,
    num_records: u32,
    free_space_remaining: u32,
    record_pointers: Vec<(u32, u32)>,
    records: Vec<Vec<u8>>,
}

impl BlockRepresentation {
    pub fn new(block: &Block) -> Self {
        let id = block.get_block_id().unwrap();
        let ptr = block.get_free_space_pointer().unwrap();
        let num = block.get_num_records().unwrap();
        let space = block.get_free_space_remaining();

        let ptrs = Vec::new();
        let records = Vec::new();

        Self {
            block_id: id,
            free_space_pointer: ptr,
            num_records: num,
            free_space_remaining: space,
            record_pointers: ptrs,
            records: records,
        }
    }
}

/// A database record with variable-length attributes.
///
/// The initial section of the record contains a null bitmap which represents
/// which attributes are null and should be ignored.
///
/// The next section of a record contains fixed-length values. Data types
/// such as numerics, booleans, and dates are encoded as is, while variable-
/// length data types such as varchars are encoded as a offset/length pair.
///
/// The actual variable-length data is stored consecutively after the initial
/// fixed-length section and null bitmap.
///
/// Data format:
/// ------------------------------------------------------------
///  NULL BITMAP | FIXED-LENGTH VALUES | VARIABLE-LENGTH VALUES
/// ------------------------------------------------------------
///
/// Metadata regarding a record is stored in a system catalog in a separate
/// database block.

pub struct Record {
    data: Vec<u8>,
}

impl Record {
    pub fn new(tmp: Vec<u8>) -> Self {
        Self { data: tmp }
    }

    pub fn len(&self) -> u32 {
        self.data.len() as u32
    }
}

pub struct Schema {}

pub struct Table {
    schema: Schema,
}
