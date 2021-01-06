/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{PageIdT, CLASSIFIER_PAGE_ID, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;
use std::sync::Mutex;

/// The disk manager is responsible for managing pages stored on disk.

pub struct DiskManager {
    db_filename: String,
    next_page_id: Mutex<PageIdT>,
}

impl DiskManager {
    /// Create a new disk manager.
    pub fn new(filename: &str) -> Self {
        Self {
            db_filename: filename.to_string(),
            next_page_id: Mutex::new(CLASSIFIER_PAGE_ID + 1),
        }
    }

    /// Write the specified byte array out to disk.
    pub fn write_page(&self, page_id: PageIdT, page_data: &[u8; PAGE_SIZE as usize]) {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.db_filename)
            .unwrap();

        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(page_data).unwrap();
        file.flush().unwrap();
    }

    /// Read a single page's data into the specified byte array.
    pub fn read_page(&self, page_id: PageIdT, page_data: &mut [u8; PAGE_SIZE as usize]) {
        let mut file = File::open(&self.db_filename).unwrap();

        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.read_exact(&mut *page_data).unwrap();
    }

    /// Allocate a page on disk and return the id of the allocated page.
    pub fn allocate_page(&self) -> u32 {
        let mut page_id = self.next_page_id.lock().unwrap();
        let next_id = *page_id;
        *page_id += 1;
        next_id
    }

    /// Deallocate the specified page on disk.
    pub fn deallocate_page(&self, _page_id: PageIdT) {}

    /// Return whether the specified page is currently allocated on disk.
    pub fn is_allocated(&self, page_id: PageIdT) -> bool {
        let next_page_id = self.next_page_id.lock().unwrap();
        page_id < *next_page_id
    }
}
