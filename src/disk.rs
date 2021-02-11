/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::constants::{PageIdT, CATALOG_ROOT_ID, PAGE_SIZE};

use crate::page::PageBytes;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;
use std::sync::atomic::{AtomicU32, Ordering};

/// The disk manager is responsible for managing pages stored on disk.

pub struct DiskManager {
    db_filename: String,
    next_page_id: AtomicU32,
}

impl DiskManager {
    /// Create a new disk manager.
    ///
    /// The first dictionary page (ID = 0) is allocated when the disk manager is initialized.
    pub fn new(filename: &str) -> Self {
        // Create database file.
        let mut file = open_write_file(filename);
        let zeros = [0; (PAGE_SIZE * 2) as usize];
        file.write_all(&zeros).unwrap();
        file.flush().unwrap();

        Self {
            db_filename: filename.to_string(),
            next_page_id: AtomicU32::new(CATALOG_ROOT_ID + 1),
        }
    }

    /// Write the specified byte array out to disk.
    pub fn write_page(&self, page_id: PageIdT, page_data: &PageBytes) {
        if !self.is_allocated(page_id) {
            panic!(
                "Cannot write page (ID: {}) which has not been allocated",
                page_id
            );
        }

        let mut file = open_write_file(&self.db_filename);
        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(page_data).unwrap();
        file.flush().unwrap();
    }

    /// Read a single page's data into the specified byte array.
    pub fn read_page(&self, page_id: PageIdT, page_data: &mut PageBytes) {
        if !self.is_allocated(page_id) {
            panic!(
                "Cannot read page (ID: {}) which has not been allocated",
                page_id
            );
        }

        let mut file = File::open(&self.db_filename).unwrap();
        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.read_exact(&mut *page_data).unwrap();
    }

    /// Allocate a page on disk and return the id of the allocated page.
    pub fn allocate_page(&self) -> u32 {
        // Open database file.
        let mut file = open_write_file(&self.db_filename);

        // Obtain the descriptor for the newly allocated page.
        let page_id = self.get_next_page_id();

        // Zero-out newly allocated page on disk.
        let data = [0; PAGE_SIZE as usize];
        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(&data).unwrap();
        file.flush().unwrap();

        // Return new page descriptor.
        page_id
    }

    /// Deallocate the specified page on disk. (Do nothing for now)
    pub fn deallocate_page(&self, _page_id: PageIdT) {}

    /// Return the next page ID and atomically increment the counter.
    fn get_next_page_id(&self) -> u32 {
        // Note: .fetch_add() increments the value and returns the PREVIOUS value
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Return whether the specified page is currently allocated on disk.
    pub fn is_allocated(&self, page_id: PageIdT) -> bool {
        page_id < self.next_page_id.load(Ordering::SeqCst)
    }
}

/// Open a file in write-mode.
pub fn open_write_file(filename: &str) -> File {
    OpenOptions::new()
        .create(true)
        .write(true)
        .open(filename)
        .unwrap()
}
