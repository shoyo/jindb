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
    let data = dm.read_page(0);
    println!("{:?}", data.unwrap().to_vec());
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

struct Page {
    data: [u8; PAGE_SIZE as usize],
}

impl Page {
    pub fn new(page_id: u32) -> Self {
        let mut page = Self {
            data: [0; PAGE_SIZE as usize],
        };
        page.set_page_id(page_id);
        page.set_free_space_pointer(PAGE_SIZE);
        page.set_num_records(0);
        page
    }

    /// Read an unsigned 32-bit integer at the specified location in the
    /// byte array.
    fn read_u32(&self, addr: u32) -> Result<u32, String> {
        if addr + 3 > PAGE_SIZE {
            return Err(format!(
                "Cannot read value from byte address address (overflow)"
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
    /// byte array. Any existing value is overwritten.
    fn write_u32(&mut self, value: u32, addr: u32) -> Result<(), String> {
        if addr + 3 > PAGE_SIZE {
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
        self.read_u32(0)
    }

    /// Set the page ID.
    fn set_page_id(&mut self, id: u32) -> Result<(), String> {
        self.write_u32(id, 0)
    }

    /// Get a pointer to the next free space.
    fn get_free_space_pointer(&self) -> Result<u32, String> {
        self.read_u32(4)
    }

    /// Set a pointer to the next free space.
    fn set_free_space_pointer(&mut self, ptr: u32) -> Result<(), String> {
        self.write_u32(ptr, 4)
    }

    /// Get the numer of records contained in the page.
    fn get_num_records(&self) -> Result<u32, String> {
        self.read_u32(8)
    }

    /// Set the number of records contained in the page.
    fn set_num_records(&mut self, num: u32) -> Result<(), String> {
        self.write_u32(num, 8)
    }

    fn insert_record(&mut self, record: Record) {}
}

struct Record {}

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
