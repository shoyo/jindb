/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::eviction_policies::clock::ClockPolicy;
use crate::buffer::eviction_policies::policy::Policy;
use crate::common::{BufferFrameIdT, PageIdT};
use crate::disk::manager::DiskManager;
use crate::page::relation_page::RelationPage;
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, Mutex, RwLock};

/// Type alias for a page protected by a R/W latch for concurrent access.
type PageLatch = Arc<RwLock<Option<RelationPage>>>;

/// The buffer manager is responsible for fetching/flushing pages that are
/// managed in memory. Any pages that don't exist in the buffer are retrieved
/// from disk through the disk manager.
pub struct BufferManager {
    /// Number of buffer frames in the buffer pool
    buffer_size: BufferFrameIdT,

    /// Collection of buffer frames that can hold guarded pages
    ///
    /// Note:
    /// Buffer pool is defined as a vector instead of a fixed-size array
    /// because of limitations with Rust syntax.
    /// To *safely* declare an N-length array of Option<T>, the options are:
    ///     1) [None; N] syntax, which requires T to implement
    ///        std::marker::Copy.
    ///     2) Using Default::default(), which requires N <= 32.
    /// Because of these limitations, the buffer pool is defined as a Vec type.
    /// The length of the vector should never change and should always be equal
    /// to self.buffer_size.
    buffer_pool: Vec<PageLatch>,

    /// Mapping from page IDs to buffer frame IDs
    page_table: Arc<Mutex<HashMap<PageIdT, BufferFrameIdT>>>,

    /// Disk manager to access pages on disk
    disk_manager: DiskManager,

    /// List of frame IDs that are not occupied
    free_list: Arc<Mutex<LinkedList<BufferFrameIdT>>>,

    /// Buffer eviction policy
    policy: ClockPolicy,
}

impl BufferManager {
    /// Construct a new buffer manager.
    pub fn new(disk_manager: DiskManager, buffer_size: BufferFrameIdT) -> Self {
        let mut pool: Vec<PageLatch> = Vec::with_capacity(buffer_size as usize);
        let mut free_list: LinkedList<BufferFrameIdT> = LinkedList::new();
        for frame_id in 0..buffer_size {
            pool.push(Arc::new(RwLock::new(None)));
            free_list.push_back(frame_id);
        }
        Self {
            buffer_size,
            buffer_pool: pool,
            page_table: Arc::new(Mutex::new(HashMap::new())),
            disk_manager,
            free_list: Arc::new(Mutex::new(free_list)),
            policy: ClockPolicy::new(),
        }
    }

    /// Initialize a new page, pin it, and return the page latch.
    /// If there are no open buffer frames and all existing pages are pinned, then
    /// return an error.
    pub fn create_page(&mut self) -> Result<PageLatch, ()> {
        // Allocate space in disk and initialize the new page.
        let page_id = self.disk_manager.allocate_page();
        let page = RelationPage::new(page_id);
        let page_latch = Arc::new(RwLock::new(Some(page)));

        // Find a frame in the buffer to house the newly created page.
        // Starting by checking the free list, which is a list of open frame IDs.
        let mut list = self.free_list.lock().unwrap();
        if list.is_empty() {
            // If free list is empty, then scan buffer frames for an unpinned page
            for i in 0..self.buffer_size {}
        } else {
            // If the free list is not empty, then pop off an index and pin the page
            // to the corresponding frame. Be sure to wrap the page in a page latch.
            let open_frame_id = list.len();
            let mut frame = self.buffer_pool[open_frame_id].write().unwrap();
        }

        Ok(page_latch.clone())
    }

    /// Fetch the specified page, pin it, and return the page latch.
    /// If the page does not exist in the buffer, then fetch the page from disk.
    /// If the page does not exist on disk, then return an error.
    pub fn fetch_page(&mut self, page_id: PageIdT) -> Result<PageLatch, ()> {
        match self._page_table_lookup(page_id) {
            // Page currently exists in the buffer, so pin it and return the latch.
            Some(frame_id) => {
                let latch = self._get_page_by_frame(frame_id).unwrap();
                let latch = match *latch.write().unwrap() {
                    Some(ref mut page) => {
                        page.pin_count += 1;
                        Ok(latch.clone())
                    }
                    None => panic!(
                        "Specified page ID {} points to an empty buffer frame",
                        page_id
                    ),
                };
                latch
            }
            // Page does not currently exist in the buffer, so fetch the page from disk.
            None => todo!(),
        }
    }

    /// Delete the specified page.
    /// If the page is pinned, then return an error.
    pub fn delete_page(&mut self, page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Flush the specified page to disk.
    pub fn flush_page(&mut self, page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Flush all pages to disk.
    pub fn flush_all_pages(&mut self) -> Result<(), ()> {
        Err(())
    }

    /// Pin the specified page to the buffer.
    /// Pinned pages will never be evicted. Threads must pin a page to the
    /// buffer before operating on it.
    pub fn pin_page(&self, page_id: PageIdT) -> Result<(), ()> {
        let frame_id = self._page_table_lookup(page_id).unwrap();
        let latch = self._get_page_by_frame(frame_id).unwrap();
        match *latch.write().unwrap() {
            Some(ref mut page) => page.pin_count += 1,
            None => panic!(
                "Attempted to pin a page contained in a page latch, but the latch contained None."
            ),
        }
        Ok(())
    }

    /// Unpin the specified page.
    /// Pages with no pins can be evicted. Threads must unpin a page when
    /// finished operating on it.
    pub fn unpin_page(&self, page_id: PageIdT) -> Result<(), String> {
        let frame_id = self._page_table_lookup(page_id).unwrap();
        let latch = self._get_page_by_frame(frame_id).unwrap();
        match *latch.write().unwrap() {
            Some(ref mut page) => {
                if page.pin_count == 0 {
                    return Err(format!("Attempted to unpin a page with a pin count of 0."));
                }
                page.pin_count -= 1;
            }
            None => panic!("Attempted to unpin a page contained in a page latch, but the latch contained None."),
        }
        Ok(())
    }

    /// Index the buffer pool and return the specified page latch.
    fn _get_page_by_frame(&self, frame_id: BufferFrameIdT) -> Result<PageLatch, String> {
        let latch = self.buffer_pool[frame_id as usize].clone();
        Ok(latch)
    }

    /// Find the specified page in the page table, and return its frame ID.
    /// If the page does not exist in the page table, then return None.
    /// Panic if the frame ID is out-of-bounds.
    fn _page_table_lookup(&self, page_id: PageIdT) -> Option<BufferFrameIdT> {
        let table = self.page_table.lock().unwrap();
        match table.get(&page_id) {
            Some(frame_id) => {
                if *frame_id >= self.buffer_size {
                    panic!(format!(
                        "Frame ID {} out of range (buffer size = {}) [broken page table]",
                        frame_id, self.buffer_size
                    ));
                }
                Some(*frame_id)
            }
            None => None,
        }
    }
}
