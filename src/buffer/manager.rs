/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::eviction_policies::clock::ClockPolicy;
use crate::buffer::eviction_policies::policy::Policy;
use crate::buffer::{Buffer, PageLatch};
use crate::common::{BufferFrameIdT, PageIdT};
use crate::disk::manager::DiskManager;
use crate::page::Page;

/// The buffer manager is responsible for fetching/flushing pages that are managed in memory.
/// Any pages that don't exist in the buffer are retrieved from disk via the disk manager.
pub struct BufferManager {
    buffer: Buffer,
    disk_manager: DiskManager,
    evict_policy: ClockPolicy,
}

impl BufferManager {
    /// Construct a new buffer manager.
    pub fn new(buffer_size: BufferFrameIdT, disk_manager: DiskManager) -> Self {
        Self {
            buffer: Buffer::new(buffer_size),
            disk_manager,
            evict_policy: ClockPolicy::new(),
        }
    }

    /// Initialize a new page, pin it, and return the page latch.
    /// If there are no open buffer frames and all existing pages are pinned, then return an error.
    pub fn create_page(&self) -> Result<PageLatch, ()> {
        // Allocate space in disk and initialize the new page.

        // Find a frame in the buffer to house the newly created page.
        // Starting by checking the free list, which is a list of open frame IDs.
        // If free list is empty, then scan buffer frames for an unpinned page
        // If the free list is not empty, then pop off an index and pin the page
        // to the corresponding frame. Be sure to wrap the page in a page latch.
        Err(())
    }

    /// Fetch the specified page, pin it, and return the page latch.
    /// If the page does not exist in the buffer, then fetch the page from disk.
    /// If the page does not exist on disk, then return an error.
    pub fn fetch_page(&self, page_id: PageIdT) -> Result<PageLatch, ()> {
        Err(())
    }

    /// Delete the specified page.
    /// If the page is pinned, then return an error.
    pub fn delete_page(&self, page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Flush the specified page to disk.
    pub fn flush_page(&self, page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Flush all pages to disk.
    pub fn flush_all_pages(&self) -> Result<(), ()> {
        Err(())
    }

    /// Pin the specified page to the buffer.
    /// Pinned pages will never be evicted. Threads must pin a page to the
    /// buffer before operating on it.
    pub fn pin_page(&self, page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Unpin the specified page.
    /// Pages with no pins can be evicted. Threads must unpin a page when
    /// finished operating on it.
    pub fn unpin_page(&self, page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Index the buffer pool and return the specified page latch.
    fn _get_page_by_frame(&self, frame_id: BufferFrameIdT) -> Result<PageLatch, ()> {
        Err(())
    }

    /// Find the specified page in the page table, and return its frame ID.
    /// If the page does not exist in the page table, then return None.
    /// Panic if the frame ID is out-of-bounds.
    fn _page_table_lookup(&self, page_id: PageIdT) -> Option<BufferFrameIdT> {
        None
    }
}
