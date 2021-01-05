/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::eviction_policies::clock::ClockPolicy;
use crate::buffer::eviction_policies::lru::LRUPolicy;
use crate::buffer::eviction_policies::slow::SlowPolicy;
use crate::buffer::eviction_policies::{EvictionPolicy, PolicyVariant};
use crate::buffer::{Buffer, PageLatch};
use crate::common::{BufferFrameIdT, PageIdT, CLASSIFIER_PAGE_ID, DICTIONARY_PAGE_ID, PAGE_SIZE};
use crate::disk::manager::DiskManager;
use crate::page::classifier_page::ClassifierPage;
use crate::page::dictionary_page::DictionaryPage;
use crate::page::relation_page::RelationPage;
use crate::page::PageVariant::Relation;
use crate::page::{Page, PageVariant};
use std::sync::{Arc, Mutex};

/// The buffer manager is responsible for managing database pages that are cached in memory.
/// Higher layers of the database system make requests to the buffer manager to create and fetch
/// pages. Any pages that don't exist in the buffer are retrieved from disk via the disk manager.
/// Multiple threads may make requests to the buffer manager in parallel, so its implementation
/// must be thread-safe.
///
/// The buffer manager manages three components to accomplish its tasks: Buffer, DiskManager,
/// and EvictionPolicy. The Buffer is an abstraction over several data structures that are each
/// guarded by a Mutex or Rwlock. The EvictionPolicy is also an abstraction over guarded data
/// structures. The disk manager is not explicitly guarded by any locks, but its API is atomic
/// and thread-safe.
pub struct BufferManager {
    buffer: Buffer,
    disk_manager: DiskManager,
    evict_policy: Box<dyn EvictionPolicy + Send + Sync>,
}

impl BufferManager {
    /// Construct a new buffer manager.
    /// Fetch necessary pages from disk to initialize in-memory data structures for page metadata.
    pub fn new(
        buffer_size: BufferFrameIdT,
        disk_manager: DiskManager,
        policy_variant: PolicyVariant,
    ) -> Self {
        // Initialize buffer data structures and eviction policy.
        let buffer = Buffer::new(buffer_size);
        let evict_policy: Box<dyn EvictionPolicy + Send + Sync> = match policy_variant {
            PolicyVariant::Clock => Box::new(ClockPolicy::new(buffer_size)),
            PolicyVariant::LRU => Box::new(LRUPolicy::new(buffer_size)),
            PolicyVariant::Slow => Box::new(SlowPolicy::new(buffer_size)),
        };

        // Fetch classifier page from disk to initialize the type chart.
        // If the classifier page is empty, nothing gets inserted into the type chart.
        let mut classifier = ClassifierPage::new();
        disk_manager.read_page(CLASSIFIER_PAGE_ID, classifier.get_data_mut());

        let rwlock = buffer.type_chart.clone();
        let mut type_chart = rwlock.write().unwrap();

        for (page_id, page_type) in classifier {
            type_chart.insert(page_id, page_type);
        }

        Self {
            buffer,
            disk_manager,
            evict_policy,
        }
    }

    /// Initialize a classifier page, pin it, and return its page latch.
    pub fn create_classifier_page(&self) -> Result<PageLatch, NoBufFrameErr> {
        self._create_page(PageVariant::Classifier)
    }

    /// Initialize a dictionary page, pin it, and return its page latch.
    pub fn create_dictionary_page(&self) -> Result<PageLatch, NoBufFrameErr> {
        self._create_page(PageVariant::Dictionary)
    }

    /// Initialize a relation page, pin it, and return its page latch.
    pub fn create_relation_page(&self) -> Result<PageLatch, NoBufFrameErr> {
        self._create_page(PageVariant::Relation)
    }

    /// Initialize a new page, pin it, and return its page latch.
    /// If there are no open buffer frames and all existing pages are pinned, then return an error.
    fn _create_page(&self, variant: PageVariant) -> Result<PageLatch, NoBufFrameErr> {
        match self.evict_policy.evict() {
            Some(frame_id) => {
                // Acquire latch for victim page.
                let page_latch = self._get_page_latch(frame_id);
                let mut frame = page_latch.write().unwrap();

                // Flush the existing page out to disk if necessary.
                if let Some(page) = frame.as_ref() {
                    self._flush_page(page);
                }

                // Allocate space on disk and initialize the new page.
                let page_id = self.disk_manager.allocate_page();
                let mut new_page: Box<dyn Page + Send + Sync> = match variant {
                    PageVariant::Classifier => Box::new(ClassifierPage::new()),
                    PageVariant::Dictionary => Box::new(DictionaryPage::new()),
                    PageVariant::Relation => Box::new(RelationPage::new(page_id)),
                };

                // Update the type chart for the new page.
                let mut type_chart = self.buffer.type_chart.write().unwrap();
                type_chart.insert(page_id, variant);

                // Update the page table and pin the new page to the buffer.
                let mut page_table = self.buffer.page_table.write().unwrap();
                page_table.insert(new_page.get_id(), frame_id);
                self.evict_policy.pin(frame_id);
                *frame = Some(new_page);

                // Return a reference to the page latch.
                Ok(page_latch.clone())
            }
            None => Err(NoBufFrameErr::new()),
        }
    }

    /// Fetch the specified page, pin it, and return its page latch.
    /// If the page does not exist in the buffer, then fetch the page from disk.
    /// If the page does not exist on disk, then return an error.
    pub fn fetch_page(&self, page_id: PageIdT) -> Result<PageLatch, NoBufFrameErr> {
        match self._page_table_lookup(page_id) {
            // If the page is already in the buffer, pin it and return its latch.
            Some(frame_id) => {
                let page_latch = self._get_page_latch(frame_id);
                self.evict_policy.pin(frame_id);
                Ok(page_latch.clone())
            }
            // Otherwise, the page must be read from disk and replace an existing page in the
            // buffer. If there aren't any pages that can be evicted, give up and return an error.
            None => match self.evict_policy.evict() {
                Some(frame_id) => {
                    let page_latch = self._get_page_latch(frame_id);
                    let frame = page_latch.write().unwrap();

                    if let Some(page) = frame.as_ref() {
                        self._flush_page(page);
                    }

                    let mut page_data = [0; PAGE_SIZE as usize];
                    self.disk_manager.read_page(page_id, &mut page_data);

                    let mut page_table = self.buffer.page_table.write().unwrap();

                    Err(NoBufFrameErr::new())
                }
                None => Err(NoBufFrameErr::new()),
            },
        }
    }

    /// Delete the specified page.
    /// If the page is pinned, then return an error.
    pub fn delete_page(&self, _page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Flush the specified page to disk. Do nothing if the page hasn't been modified.
    ///
    /// Note: This method in isolation is NOT thread-safe. This method should only be called in
    /// the context of another buffer manager method which is thread-safe.
    fn _flush_page(&self, page: &Box<dyn Page + Send + Sync>) {
        self.disk_manager
            .write_page(page.get_id(), page.get_data())
            .unwrap();
    }

    /// Flush all pages to disk.
    pub fn flush_all_pages(&self) -> Result<(), ()> {
        Err(())
    }

    /// Unpin the specified page.
    /// Pages with no pins can be evicted. Threads must unpin a page when
    /// finished operating on it.
    pub fn unpin_page(&self, _page_id: PageIdT) -> Result<(), ()> {
        Err(())
    }

    /// Index the buffer pool and return the specified page latch.
    fn _get_page_latch(&self, frame_id: BufferFrameIdT) -> PageLatch {
        self.buffer.pool[frame_id as usize].clone()
    }

    /// Find the specified page in the page table, and return its frame ID.
    /// If the page does not exist in the page table, then return None.
    /// Panic if the frame ID is out-of-bounds.
    fn _page_table_lookup(&self, page_id: PageIdT) -> Option<BufferFrameIdT> {
        let page_table = self.buffer.page_table.read().unwrap();
        match page_table.get(&page_id) {
            Some(frame_id) => Some(*frame_id),
            None => None,
        }
    }
}

/// Custom error types to be used by the buffer manager.

/// Error to be thrown when no buffer frames are open, and every page occupying a buffer frame is
/// pinned and cannot be evicted.
#[derive(Debug)]
pub struct NoBufFrameErr {
    msg: String,
}

impl NoBufFrameErr {
    fn new() -> Self {
        Self {
            msg: format!("No available buffer frames, and all pages are pinned"),
        }
    }
}
