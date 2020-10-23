use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;

const PAGE_SIZE: u32 = 64;
const DB_FILENAME: &str = "db.minusql";

fn main() {
    println!("minuSQL (2020)");
    println!("Enter .help for usage hints");

    let mut page = Page::new(0);
    page.set_page_id(654321);
    let id = page.get_page_id().unwrap();
    println!("read id: {}", id);

    let dm = DiskManager::new();
    let page_data = dm.read_page(0);
    println!("{:?}", page_data.unwrap().to_vec());
}

/// A database page with slotted-page architecture.
/// Stores a header and variable-length records that grow in opposite
/// directions, similarly to a heap and stack.
///
/// Data format:
/// --------------------------------------------------------
///  HEADER (grows ->) | ... FREE ... | (<- grows) RECORDS
/// --------------------------------------------------------
///                                   ^ Free Space Pointer
///
///
/// Header stores metadata (number denotes size in bytes):
/// --------------------------------------------------------
///  PAGE ID (4) | FREE SPACE POINTER (4) | NUM RECORDS (4)
/// --------------------------------------------------------
/// --------------------------------------------------------
///  RECORD 1 LENGTH (4) | RECORD 1 OFFSET (4) |    ...
/// --------------------------------------------------------
///
///
/// Records:
/// --------------------------------------------------------
///          ...           | RECORD 3 | RECORD 2 | RECORD 1
/// --------------------------------------------------------

const PAGE_ID_OFFSET: u32 = 0;
const FREE_POINTER_OFFSET: u32 = 4;
const NUM_RECORDS_OFFSET: u32 = 8;
const RECORDS_OFFSET: u32 = 12;
const RECORD_POINTER_SIZE: u32 = 8;

struct Page {
    data: [u8; PAGE_SIZE as usize],
}

impl Page {
    pub fn new(page_id: u32) -> Self {
        let mut page = Self {
            data: [0; PAGE_SIZE as usize],
        };
        page.set_page_id(page_id);
        page.set_free_space_pointer(PAGE_SIZE - 1);
        page.set_num_records(0);
        page
    }

    /// Read an unsigned 32-bit integer at the specified location in the
    /// byte array.
    fn read_u32(&self, addr: u32) -> Result<u32, String> {
        if addr + 4 > PAGE_SIZE {
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
    fn write_u32(&mut self, value: u32, addr: u32) -> Result<(), String> {
        if addr + 4 > PAGE_SIZE {
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

    /// Get the page ID.
    fn get_page_id(&self) -> Result<u32, String> {
        self.read_u32(PAGE_ID_OFFSET)
    }

    /// Set the page ID.
    fn set_page_id(&mut self, id: u32) -> Result<(), String> {
        self.write_u32(id, PAGE_ID_OFFSET)
    }

    /// Get a pointer to the next free space.
    fn get_free_space_pointer(&self) -> Result<u32, String> {
        self.read_u32(FREE_POINTER_OFFSET)
    }

    /// Set a pointer to the next free space.
    fn set_free_space_pointer(&mut self, ptr: u32) -> Result<(), String> {
        self.write_u32(ptr, FREE_POINTER_OFFSET)
    }

    /// Get the numer of records contained in the page.
    fn get_num_records(&self) -> Result<u32, String> {
        self.read_u32(NUM_RECORDS_OFFSET)
    }

    /// Set the number of records contained in the page.
    fn set_num_records(&mut self, num: u32) -> Result<(), String> {
        self.write_u32(num, NUM_RECORDS_OFFSET)
    }

    /// Calculate the amount of free space (in bytes) left in the page.
    fn get_free_space_remaining(&self) -> u32 {
        let free_ptr = self.get_free_space_pointer().unwrap();
        let num_records = self.get_num_records().unwrap();
        free_ptr + 1 - RECORDS_OFFSET - num_records * RECORD_POINTER_SIZE
    }

    /// Insert a record in the page and update the header.
    fn insert_record(&mut self, record: Record) -> Result<(), String> {
        // Calculate header addresses for new length/offset entry
        let num_records = self.get_num_records().unwrap();
        let length_addr = RECORDS_OFFSET + num_records * RECORD_POINTER_SIZE;
        let offset_addr = length_addr + 4;

        // Bounds-check for record insertion
        let free_ptr = self.get_free_space_pointer().unwrap();
        let new_free_ptr = free_ptr - record.len();
        if new_free_ptr < offset_addr + 4 {
            return Err(format!(
                "Overflow: Record does not fit in page (ID={})",
                self.get_page_id().unwrap()
            ));
        }

        // Write record data to allocated space
        let start = (new_free_ptr + 1) as usize;
        let end = (free_ptr + 1) as usize;
        for i in start..end {
            self.data[i] = record.data[i];
        }

        // Update header
        self.write_u32(length_addr, record.len());
        self.write_u32(offset_addr, new_free_ptr + 1);
        self.set_free_space_pointer(new_free_ptr);
        self.set_num_records(num_records + 1);

        Ok(())
    }

    /// Update a record in the page.
    fn update_record(&mut self, record: Record) {}
}

struct Record {
    data: Vec<u8>,
}

impl Record {
    fn new() -> Self {
        Self { data: Vec::new() }
    }

    fn len(&self) -> u32 {
        self.data.len() as u32
    }
}

struct DiskManager;

impl DiskManager {
    pub fn new() -> Self {
        Self
    }

    fn write_page(&self, page_id: u32, page: Page) -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(DB_FILENAME)?;

        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
        file.write_all(&page.data);
        file.flush()?;

        Ok(())
    }

    fn read_page(&self, page_id: u32) -> std::io::Result<[u8; PAGE_SIZE as usize]> {
        let mut file = File::open(DB_FILENAME)?;

        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
        let mut buf = [0; PAGE_SIZE as usize];
        file.read_exact(&mut buf)?;

        Ok(buf)
    }
}
