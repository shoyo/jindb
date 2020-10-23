use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;

const PAGE_SIZE: u32 = 512;
const DB_FILENAME: &str = "db.minusql";

fn main() {
    println!("minuSQL (2020)");
    println!("Enter .help for usage hints");
    //    loop {
    //        print!("minuSQL > ");
    //        io::stdout().flush().unwrap();
    //
    //        let mut query = String::new();
    //        io::stdin()
    //            .read_line(&mut query)
    //            .expect("Error reading input");
    //        println!("TODO");
    //    }
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

    fn set_page_id(&mut self, id: u32) {}

    fn set_free_space_pointer(&mut self, offset: u32) {}

    fn set_num_records(&mut self, num: u32) {}
}

struct Record {}

struct DiskManager;

impl DiskManager {
    pub fn new() -> Self {
        Self
    }

    fn write_page(page_id: u32, page: Page) -> std::io::Result<()> {
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

    fn read_page(page_id: u32) -> std::io::Result<[u8; PAGE_SIZE as usize]> {
        let mut file = File::open(DB_FILENAME)?;

        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
        let mut buf = [0; PAGE_SIZE as usize];
        file.read_exact(&mut buf)?;

        Ok(buf)
    }
}
