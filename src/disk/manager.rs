/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{DICTIONARY_PAGE_ID, PAGE_SIZE};
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
    pub fn new(filename: &str) -> Self {
        Self {
            db_filename: filename.to_string(),
            next_page_id: AtomicU32::new(DICTIONARY_PAGE_ID + 1),
        }
    }

    /// Write the specified byte array out to disk.
    pub fn write_page(
        &self,
        page_id: u32,
        page_data: &[u8; PAGE_SIZE as usize],
    ) -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.db_filename)?;

        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset as u64))?;
        file.write_all(page_data)?;
        file.flush()?;

        Ok(())
    }

    /// Read a single page's data into the specified byte array.
    pub fn read_page(
        &self,
        page_id: u32,
        page_data: &mut [u8; PAGE_SIZE as usize],
    ) -> std::io::Result<()> {
        let mut file = File::open(&self.db_filename)?;

        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset as u64))?;
        file.read_exact(&mut *page_data)?;

        Ok(())
    }

    /// Allocate a page on disk and return the id of the allocated page.
    pub fn allocate_page(&self) -> u32 {
        // Note: .fetch_add() increments the value and returns the PREVIOUS value
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Deallocate the specified page on disk.
    pub fn deallocate_page(&self, _page_id: u32) -> Result<(), ()> {
        Ok(())
    }
}
