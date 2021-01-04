/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::eviction_policies::clock::ClockPolicy;
use crate::buffer::eviction_policies::lru::LRUPolicy;
use crate::buffer::eviction_policies::slow::SlowPolicy;
use crate::buffer::eviction_policies::{EvictionPolicy, PolicyVariant};
use crate::buffer::{Buffer, PageLatch};
use crate::common::{BufferFrameIdT, PageIdT};
use crate::disk::manager::DiskManager;
use crate::page::dictionary_page::DictionaryPage;
use crate::page::relation_page::RelationPage;
use crate::page::{Page, PageVariant};
use std::sync::{Arc, Mutex};

/// The buffer manager is responsible for fetching/flushing pages that are managed in memory.
/// Any pages that don't exist in the buffer are retrieved from disk via the disk manager.
/// Multiple threads may make requests to the buffer manager in parallel, so its implementation
/// must be thread-safe.
///
/// The buffer manager manages three components to accomplish its tasks: Buffer, DiskManager,
/// and EvictionPolicy. The Buffer is an abstraction over several data structures that are each
/// guarded by a Mutex or Rwlock. The EvictionPolicy is also an abstraction over guarded data
/// structures.The disk manager is not explicitly guarded by any locks, but its API is (should
/// be) atomic and thread-safe.
pub struct BufferManager {
    buffer: Buffer,
    disk_manager: DiskManager,
    evict_policy: Arc<Mutex<Box<dyn EvictionPolicy>>>,
}

impl BufferManager {
    /// Construct a new buffer manager.
    pub fn new(
        buffer_size: BufferFrameIdT,
        disk_manager: DiskManager,
        evict_policy: PolicyVariant,
    ) -> Self {
        let policy: Box<dyn EvictionPolicy> = match evict_policy {
            PolicyVariant::Clock => Box::new(ClockPolicy::new(buffer_size)),
            PolicyVariant::LRU => Box::new(LRUPolicy::new(buffer_size)),
            PolicyVariant::Slow => Box::new(SlowPolicy::new(buffer_size)),
        };
        Self {
            buffer: Buffer::new(buffer_size),
            disk_manager,
            evict_policy: Arc::new(Mutex::new(policy)),
        }
    }

    /// Initialize a relation page, pin it, and return its page latch.
    pub fn create_relation_page(&self) -> Result<PageLatch, BufFrameErr> {
        self._create_page(PageVariant::Relation)
    }

    /// Initialize a dictionary page, pin it, and return its page latch.
    pub fn create_dictionary_page(&self) -> Result<PageLatch, BufFrameErr> {
        self._create_page(PageVariant::Dictionary)
    }

    /// Initialize a new page, pin it, and return its page latch.
    /// If there are no open buffer frames and all existing pages are pinned, then return an error.
    fn _create_page(&self, variant: PageVariant) -> Result<PageLatch, BufFrameErr> {
        // Allocate space in disk and initialize the new page.
        let page_id = self.disk_manager.allocate_page();

        // Find a frame in the buffer to house the newly created page.
        // Start by checking the free list, which is a list of open frame IDs.
        let mut free_list = self.buffer.free_list.lock().unwrap();
        match free_list.pop_front() {
            // If the free list is not empty, pop off the first item and pin the page to the
            // corresponding buffer frame.
            Some(frame_id) => {
                let page_latch = self.buffer.pool[frame_id as usize].clone();
                let mut frame = page_latch.write().unwrap();
                let mut page_table = self.buffer.page_table.write().unwrap();

                let mut new_page: Box<dyn Page> = match variant {
                    PageVariant::Dictionary => Box::new(DictionaryPage::new()),
                    PageVariant::Relation => Box::new(RelationPage::new(page_id)),
                };

                let policy = self.evict_policy.lock().unwrap();
                new_page.incr_pin_count();
                policy.pin(new_page.get_id());
                *frame = Some(new_page);
                page_table.insert(page_id, frame_id);

                Ok(page_latch.clone())
            }
            // If the free list is empty, then refer to the eviction policy to choose a victim page.
            None => {
                let policy = self.evict_policy.lock().unwrap();
                match policy.evict() {
                    // If a page can be evicted, flush out the victim page to disk if necessary
                    // and overwrite the buffer frame.
                    Some(frame_id) => {
                        let page_latch = self.buffer.pool[frame_id as usize].clone();
                        let mut frame = page_latch.write().unwrap();
                        let mut page_table = self.buffer.page_table.write().unwrap();
                        let page = frame.as_ref().unwrap(); // Frame is guaranteed to be Some.

                        if page.is_dirty() {
                            self.disk_manager
                                .write_page(page.get_id(), page.get_data())
                                .unwrap();
                        }

                        let mut new_page: Box<dyn Page> = match variant {
                            PageVariant::Dictionary => Box::new(DictionaryPage::new()),
                            PageVariant::Relation => Box::new(RelationPage::new(page_id)),
                        };
                        let policy = self.evict_policy.lock().unwrap();
                        new_page.incr_pin_count();
                        policy.pin(new_page.get_id());
                        *frame = Some(new_page);
                        page_table.insert(page_id, frame_id);

                        Ok(page_latch.clone())
                    }
                    // If no page can be evicted, then give up and return an error.
                    None => Err(BufFrameErr::new()),
                }
            }
        }
    }

    /// Fetch the specified page, pin it, and return its page latch.
    /// If the page does not exist in the buffer, then fetch the page from disk.
    /// If the page does not exist on disk, then return an error.
    pub fn fetch_page(&self, _page_id: PageIdT) -> Result<PageLatch, ()> {
        Err(())
    }

    /// Delete the specified page.
    /// If the page is pinned, then return an error.
    pub fn delete_page(&self, _page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Flush the specified page to disk.
    pub fn flush_page(&self, _page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Flush all pages to disk.
    pub fn flush_all_pages(&self) -> Result<(), ()> {
        Err(())
    }

    /// Pin the specified page to the buffer.
    /// Pinned pages will never be evicted. Threads must pin a page to the
    /// buffer before operating on it.
    pub fn pin_page(&self, _page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Unpin the specified page.
    /// Pages with no pins can be evicted. Threads must unpin a page when
    /// finished operating on it.
    pub fn unpin_page(&self, _page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Index the buffer pool and return the specified page latch.
    fn _get_page_by_frame(&self, _frame_id: BufferFrameIdT) -> Result<PageLatch, ()> {
        Err(())
    }

    /// Find the specified page in the page table, and return its frame ID.
    /// If the page does not exist in the page table, then return None.
    /// Panic if the frame ID is out-of-bounds.
    fn _page_table_lookup(&self, _page_id: PageIdT) -> Option<BufferFrameIdT> {
        None
    }
}

/// Custom error types to be used by the buffer manager.

/// Error to be thrown when no buffer frames are open, and every page occupying a buffer frame is
/// pinned and cannot be evicted.
#[derive(Debug)]
pub struct BufFrameErr {
    msg: String,
}

impl BufFrameErr {
    fn new() -> Self {
        Self {
            msg: format!("No available buffer frames, and all pages are pinned"),
        }
    }
}
