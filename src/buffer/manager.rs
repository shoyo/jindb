/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::replacement::clock::ClockReplacer;
use crate::buffer::replacement::lru::LRUReplacer;
use crate::buffer::replacement::slow::SlowReplacer;
use crate::buffer::replacement::{PageReplacer, ReplacerAlgorithm};
use crate::buffer::{Buffer, BufferFrame, FrameLatch};
use crate::common::{BufferFrameIdT, PageIdT, CLASSIFIER_PAGE_ID};
use crate::disk::manager::DiskManager;
use crate::page::classifier_page::ClassifierPage;

use crate::page::{init_page_variant, Page, PageVariant};
use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockWriteGuard};

/// The buffer manager is responsible for managing database pages that are cached in memory.
/// Higher layers of the database system make requests to the buffer manager to create and fetch
/// pages. Any pages that don't exist in the buffer are retrieved from disk via the disk manager.
/// Multiple threads may make requests to the buffer manager in parallel, so its implementation
/// must be thread-safe.

pub struct BufferManager {
    /// A pool of buffer frames to hold database pages
    buffer: Buffer,

    /// Disk manager for reading from and writing to disk
    disk_manager: DiskManager,

    /// Page replacement manager
    replacer: Box<dyn PageReplacer + Send + Sync>,

    /// Mapping of pages to buffer frames that they occupy
    page_table: Arc<RwLock<HashMap<PageIdT, BufferFrameIdT>>>,

    /// Mapping of pages to their page variants
    type_chart: Arc<RwLock<HashMap<PageIdT, PageVariant>>>,
}

impl BufferManager {
    /// Construct a new buffer manager.
    /// Fetch necessary pages from disk to initialize in-memory data structures for page metadata.
    pub fn new(
        buffer_size: BufferFrameIdT,
        disk_manager: DiskManager,
        replacer_algorithm: ReplacerAlgorithm,
    ) -> Self {
        // Initialize page replacement manager
        let replacer: Box<dyn PageReplacer + Send + Sync> = match replacer_algorithm {
            ReplacerAlgorithm::Clock => Box::new(ClockReplacer::new(buffer_size)),
            ReplacerAlgorithm::LRU => Box::new(LRUReplacer::new(buffer_size)),
            ReplacerAlgorithm::Slow => Box::new(SlowReplacer::new(buffer_size)),
        };

        // Fetch classifier page from disk to initialize the type chart.
        // If the classifier page is empty, nothing gets inserted into the type chart.
        let mut classifier = ClassifierPage::new(CLASSIFIER_PAGE_ID);
        disk_manager.read_page(CLASSIFIER_PAGE_ID, classifier.as_mut_bytes());
        let mut type_chart = HashMap::new();
        for (page_id, page_type) in classifier {
            type_chart.insert(page_id, page_type);
        }

        Self {
            buffer: Buffer::new(buffer_size),
            disk_manager,
            replacer,
            page_table: Arc::new(RwLock::new(HashMap::new())),
            type_chart: Arc::new(RwLock::new(type_chart)),
        }
    }

    /// Initialize a classifier page, pin it, and return its page latch.
    pub fn create_classifier_page(&self) -> Result<FrameLatch, BufferError> {
        self._create_page(PageVariant::Classifier)
    }

    /// Initialize a dictionary page, pin it, and return its page latch.
    pub fn create_dictionary_page(&self) -> Result<FrameLatch, BufferError> {
        self._create_page(PageVariant::Dictionary)
    }

    /// Initialize a relation page, pin it, and return its page latch.
    pub fn create_relation_page(&self) -> Result<FrameLatch, BufferError> {
        self._create_page(PageVariant::Relation)
    }

    /// Initialize a new page, pin it, and return its page latch.
    /// If there are no open buffer frames and all existing pages are pinned, then return an error.
    fn _create_page(&self, variant: PageVariant) -> Result<FrameLatch, BufferError> {
        match self.replacer.evict() {
            Some(frame_id) => {
                // Acquire latch for victim page.
                let frame_latch = self.buffer.get(frame_id);
                let mut frame = frame_latch.write().unwrap();

                // Assert that selected page is a valid victim page.
                // TODO: handle pin assertions in page replacer
                frame.assert_no_pins();

                // Allocate space on disk and initialize the new page.
                let page_id = self.disk_manager.allocate_page();
                let new_page = init_page_variant(page_id, variant);

                // Acquire locks for page table and type chart (in this order).
                let mut page_table = self.page_table.write().unwrap();
                let mut type_chart = self.type_chart.write().unwrap();

                // Update the page table and type chart.
                if let Some(victim_page) = frame.get_page() {
                    page_table.remove(&victim_page.get_id());
                }
                page_table.insert(new_page.get_id(), frame_id);
                type_chart.insert(page_id, variant);

                // Write the existing page out to disk and reset the buffer frame.
                self._flush_frame(&*frame);
                frame.reset();

                // Place the new page in the buffer frame and pin it.
                frame.overwrite(Some(new_page));
                frame.pin();
                self.replacer.pin(frame_id);

                // Return a reference to the page latch.
                Ok(frame_latch.clone())
            }
            None => Err(BufferError::NoBufFrame),
        }
    }

    /// Fetch the specified page, pin it, and return its page latch.
    /// If the page does not exist in the buffer, then fetch the page from disk.
    /// If the page does not exist on disk, then return an error.
    pub fn fetch_page(&self, page_id: PageIdT) -> Result<FrameLatch, BufferError> {
        if !self.disk_manager.is_allocated(page_id) {
            return Err(BufferError::PageDiskDNE);
        }
        match self._page_table_lookup(page_id) {
            // If the page is already in the buffer, pin it and return its latch.
            Some(frame_latch) => {
                let mut frame = frame_latch.write().unwrap();
                frame.pin();
                self.replacer.pin(frame.id);
                Ok(frame_latch.clone())
            }
            // Otherwise, the page must be read from disk and replace an existing page in the
            // buffer. If there aren't any pages that can be evicted, give up and return an error.
            None => match self.replacer.evict() {
                Some(frame_id) => {
                    // Acquire latch for victim page.
                    let frame_latch = self.buffer.get(frame_id);
                    let mut frame = frame_latch.write().unwrap();

                    // Assert that selected page is a valid victim page.
                    // TODO: handle pin assertions in page replacer
                    frame.assert_no_pins();

                    // Acquire locks for page table and type chart (in this order).
                    let mut page_table = self.page_table.write().unwrap();
                    let type_chart = self.type_chart.read().unwrap();

                    // Fetch the requested page into memory from disk.
                    let mut new_page: Box<dyn Page + Send + Sync> = match type_chart.get(&page_id) {
                        Some(variant) => init_page_variant(page_id, *variant),
                        None => panic!("Page ID {} does not have a type chart entry", page_id),
                    };
                    self.disk_manager
                        .read_page(page_id, new_page.as_mut_bytes());

                    // Update the page table.
                    if let Some(victim_page) = frame.get_page() {
                        page_table.remove(&victim_page.get_id());
                    }
                    page_table.insert(page_id, frame_id).unwrap();

                    // Write the existing page out to disk and reset the buffer frame.
                    self._flush_frame(&*frame);
                    frame.reset();

                    // Place the new page in the buffer frame and pin it.
                    frame.overwrite(Some(new_page));
                    frame.pin();
                    self.replacer.pin(frame_id);

                    // Return a reference to the page latch.
                    Ok(frame_latch.clone())
                }
                None => Err(BufferError::NoBufFrame),
            },
        }
    }

    /// Delete the specified page. If the page is pinned, then return an error.
    pub fn delete_page(&self, page_id: PageIdT) -> Result<(), BufferError> {
        match self._page_table_lookup(page_id) {
            Some(frame_latch) => {
                let mut frame = frame_latch.write().unwrap();

                match frame.get_pin_count() {
                    0 => {
                        let mut page_table = self.page_table.write().unwrap();
                        let mut type_chart = self.type_chart.write().unwrap();
                        page_table.remove(&page_id).unwrap();
                        type_chart.remove(&page_id).unwrap();

                        self.disk_manager.deallocate_page(page_id);
                        self.replacer.unpin(frame.get_id());
                        frame.reset();
                        Ok(())
                    }
                    _ => Err(BufferError::PagePinned),
                }
            }
            None => {
                self.disk_manager.deallocate_page(page_id);
                Ok(())
            }
        }
    }

    /// Unpin the page contained in the specified frame and drop the write guard.
    ///
    /// This method is intended to be used by the same thread that has already gained exclusive
    /// access to a frame after invoking .write() on a frame latch received through a fetch or
    /// create request. Instead of dropping its WriteGuard and needing to reacquire it to
    /// perform an unpin, it uses this method instead to reduce overhead.
    pub fn unpin_and_drop(&self, mut frame: RwLockWriteGuard<BufferFrame>) {
        match frame.get_page() {
            Some(_) => {
                frame.unpin();
                if frame.get_pin_count() == 0 {
                    self.replacer.unpin(frame.get_id());
                }
            }
            None => panic!("Attempted to unpin an empty buffer frame"),
        }
        drop(frame); // Not actually needed, but makes operation explicit.
    }

    /// Flush the specified page to disk. Return an error if the page does not exist in the buffer.
    pub fn flush_page(&self, page_id: PageIdT) -> Result<(), BufferError> {
        match self._page_table_lookup(page_id) {
            Some(frame_latch) => {
                let frame = frame_latch.read().unwrap();
                self._flush_frame(&*frame);
                Ok(())
            }
            None => Err(BufferError::PageBufDNE),
        }
    }

    /// Flush all pages to disk.
    pub fn flush_all_pages(&self) {
        for frame_id in 0..self.buffer.size() {
            let frame_latch = self.buffer.get(frame_id);
            let frame = frame_latch.read().unwrap();
            self._flush_frame(&*frame);
        }
    }

    /// Flush the page contained in the specified frame latch to disk. Do nothing if the page has
    /// not been modified.
    fn _flush_frame(&self, frame: &BufferFrame) {
        if frame.is_dirty() {
            // Unwrapping is okay here because a dirty frame implies that a page is contained in
            // the frame.
            let page = frame.get_page().unwrap();
            self.disk_manager.write_page(page.get_id(), page.as_bytes())
        }
    }

    /// Find the specified page in the page table, and return its frame latch.
    /// If the page does not exist in the page table, then return None.
    /// Panic if a frame that the page table points to is empty or contains the wrong page.
    fn _page_table_lookup(&self, page_id: PageIdT) -> Option<FrameLatch> {
        let page_table = self.page_table.read().unwrap();
        match page_table.get(&page_id) {
            Some(frame_id) => {
                let frame_latch = self.buffer.get(*frame_id);
                let frame = frame_latch.read().unwrap();
                match frame.get_page() {
                    Some(ref page) => {
                        if page.get_id() != page_id {
                            panic!(
                                "Broken page table: expected page ID {}, got page ID {}",
                                page_id,
                                page.get_id()
                            )
                        }
                        Some(frame_latch.clone())
                    }
                    None => panic!("Broken page table: frame ID {} is empty", frame_id),
                }
            }
            None => None,
        }
    }
}

/// Custom error types to be used by the buffer manager.

#[derive(Debug)]
pub enum BufferError {
    /// Error to be thrown when no buffer frames are open, and every page occupying a buffer frame is
    /// pinned and cannot be evicted.
    NoBufFrame,

    /// Error to be thrown when a page that is pinned and an operation cannot proceed.
    PagePinned,

    /// Error to be thrown when the specified page does not exist in the buffer.
    PageBufDNE,

    /// Error to be thrown when the specified page does not exist on disk.
    PageDiskDNE,
}
