/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{PageIdT, CLASSIFIER_PAGE_ID, PAGE_SIZE};
use crate::disk::open_write_file;
use std::fs::File;
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
    ///
    /// The first pages of both the dictionary page and classifier page are allocated when the
    /// disk manager is initialized. (Pages with ID = 0 and ID = 2)
    pub fn new(filename: &str) -> Self {
        // Create database file.
        let mut file = open_write_file(filename);
        let zeros = [0; (PAGE_SIZE * 2) as usize];
        file.write_all(&zeros).unwrap();
        file.flush().unwrap();

        Self {
            db_filename: filename.to_string(),
            next_page_id: Mutex::new(CLASSIFIER_PAGE_ID + 1),
        }
    }

    /// Write the specified byte array out to disk.
    pub fn write_page(&self, page_id: PageIdT, page_data: &[u8; PAGE_SIZE as usize]) {
        if !self.is_allocated(page_id) {
            panic!("Cannot write page (ID: {}) which has not been allocated");
        }

        let mut file = open_write_file(&self.db_filename);
        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(page_data).unwrap();
        file.flush().unwrap();
    }

    /// Read a single page's data into the specified byte array.
    pub fn read_page(&self, page_id: PageIdT, page_data: &mut [u8; PAGE_SIZE as usize]) {
        if !self.is_allocated(page_id) {
            panic!("Cannot read page (ID: {}) which has not been allocated");
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
        let mut page_id = self.next_page_id.lock().unwrap();
        let alloc_id = *page_id;
        *page_id += 1;

        // Zero-out newly allocated page on disk.
        let data = [0; PAGE_SIZE as usize];
        let offset = alloc_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(&data).unwrap();
        file.flush().unwrap();

        // Return new page descriptor.
        alloc_id
    }

    /// Deallocate the specified page on disk. (Do nothing for now)
    pub fn deallocate_page(&self, _page_id: PageIdT) {}

    /// Return whether the specified page is currently allocated on disk.
    pub fn is_allocated(&self, page_id: PageIdT) -> bool {
        let next_page_id = self.next_page_id.lock().unwrap();
        page_id < *next_page_id
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{CLASSIFIER_PAGE_ID, DICTIONARY_PAGE_ID, PAGE_SIZE};
    use crate::disk::manager::DiskManager;
    use crate::disk::open_write_file;
    use std::convert::TryInto;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::{Arc, Barrier};
    use std::{fs, thread};

    struct TestContext {
        disk_manager: DiskManager,
        filename: String,
    }

    impl TestContext {
        fn new(filename: &str) -> Self {
            Self {
                disk_manager: DiskManager::new(filename),
                filename: filename.to_string(),
            }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            fs::remove_file(&self.filename);
        }
    }

    fn setup(test_id: usize) -> TestContext {
        let filename = format!("DM_TEST_{}", test_id);
        TestContext {
            disk_manager: DiskManager::new(&filename),
            filename,
        }
    }

    #[test]
    fn test_disk_allocation() {
        let mut ctx = setup(0);
        let manager = &mut ctx.disk_manager;

        assert_eq!(manager.is_allocated(DICTIONARY_PAGE_ID), true);
        assert_eq!(manager.is_allocated(CLASSIFIER_PAGE_ID), true);
        assert_eq!(manager.is_allocated(2), false);

        let page_id = manager.allocate_page();
        assert_eq!(page_id, 2);
        assert_eq!(manager.is_allocated(2), true);
    }

    #[test]
    fn test_disk_write() {
        let ctx = setup(1);

        // Write expected data to disk with disk manager.
        let expected = [123; PAGE_SIZE as usize];
        let page_id = ctx.disk_manager.allocate_page();
        ctx.disk_manager.write_page(page_id, &expected);

        // Manually read page data from disk.
        let mut actual = [0; PAGE_SIZE as usize];
        let mut file = File::open(&ctx.filename).unwrap();
        file.seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64))
            .unwrap();
        file.read_exact(&mut actual).unwrap();
        file.flush().unwrap();

        // Assert that actual data matches expected data.
        for i in 0..PAGE_SIZE as usize {
            assert_eq!(actual[i], expected[i]);
        }
    }

    #[test]
    fn test_disk_read() {
        let ctx = setup(3);

        // Manually write page data to disk.
        let mut file = open_write_file(&ctx.filename);
        let page_id = ctx.disk_manager.allocate_page();
        file.seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64))
            .unwrap();
        for i in 0..=255 {
            let byte = file.write(&[i]).unwrap();
            assert_eq!(byte, 1);
        }

        // Read page data from disk with disk manager.
        let mut data = [0; PAGE_SIZE as usize];
        ctx.disk_manager.read_page(page_id, &mut data);

        // Assert that actual data matches expected data.
        for i in 0..=255 {
            assert_eq!(data[i], i as u8);
        }
    }

    #[test]
    #[should_panic]
    fn test_unallocated_read() {
        let ctx = setup(4);
        ctx.disk_manager.read_page(2, &mut [0; PAGE_SIZE as usize]);
    }

    #[test]
    #[should_panic]
    fn test_unallocated_write() {
        let ctx = setup(5);
        ctx.disk_manager.write_page(2, &[0; PAGE_SIZE as usize]);
    }

    #[test]
    /// Assert that multiple threads can read the same page from disk simultaneously.
    fn test_concurrent_read_access() {
        let ctx = Arc::new(setup(6));
        let num_threads = 10;

        // Write data to a page on disk.
        let expected = [213; PAGE_SIZE as usize];
        ctx.disk_manager.write_page(CLASSIFIER_PAGE_ID, &expected);

        // Spin up multiple threads, and make each thread independently read the same page into
        // memory. Assert that each thread obtains the correct data.
        for _ in 0..num_threads {
            let ctx_c = ctx.clone();
            thread::spawn(move || {
                let mut actual = [0; PAGE_SIZE as usize];
                ctx_c
                    .disk_manager
                    .read_page(CLASSIFIER_PAGE_ID, &mut actual);

                for i in 0..PAGE_SIZE as usize {
                    assert_eq!(actual[i], expected[i]);
                }
            });
        }
    }

    #[test]
    /// Assert that multiple threads can allocate and write to different pages on disk
    /// simultaneously.
    fn test_concurrent_write_access() {
        let ctx = Arc::new(setup(7));
        let num_threads = 10;

        // Spin up multiple threads, and make each thread allocate a new page on disk.
        // Have each thread write some unique data to their corresponding page.
        let mut handles = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let ctx_c = ctx.clone();
            handles.push(thread::spawn(move || {
                let page_id = ctx_c.disk_manager.allocate_page();

                // Write the page's ID to each byte of the newly allocated page.
                ctx_c
                    .disk_manager
                    .write_page(page_id, &[page_id.try_into().unwrap(); PAGE_SIZE as usize]);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert that allocations were successful.
        assert!(ctx
            .disk_manager
            .is_allocated(CLASSIFIER_PAGE_ID + num_threads as u32));

        // Spin up a new set of threads, and make all threads access a different disk page
        // simultaneously. Assert that each page contains the correct data.
        let mut handles = Vec::with_capacity(num_threads);
        let barrier = Arc::new(Barrier::new(num_threads));

        for i in 1..(num_threads + 1) as u32 {
            let ctx_c = ctx.clone();
            let bar = barrier.clone();
            handles.push(thread::spawn(move || {
                let mut data = [0; PAGE_SIZE as usize];

                bar.wait(); // Sync all threads

                // Assert that each byte of the page is the page's ID.
                ctx_c
                    .disk_manager
                    .read_page(CLASSIFIER_PAGE_ID + i, &mut data);

                for j in 0..PAGE_SIZE as usize {
                    assert_eq!(data[j], (CLASSIFIER_PAGE_ID + i) as u8);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
