/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::replacement::clock::ClockReplacer;
use crate::buffer::replacement::lru::LRUReplacer;
use crate::buffer::replacement::slow::SlowReplacer;
use crate::buffer::replacement::{PageReplacer, ReplacerAlgorithm};
use crate::buffer::{Buffer, BufferFrame, FrameArc, FrameLatch, FrameRLatch, FrameWLatch};
use crate::common::{BufferFrameIdT, PageIdT, BUFFER_SIZE, CLASSIFIER_PAGE_ID};
use crate::disk::manager::DiskManager;
use crate::page::classifier_page::ClassifierPage;

use crate::page::{init_page_variant, Page, PageVariant};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Type alias for read and write latches returned by the buffer manager.
type PageTable = HashMap<PageIdT, BufferFrameIdT>;
type TypeChart = HashMap<PageIdT, PageVariant>;

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
    page_table: Arc<Mutex<PageTable>>,

    /// Mapping of pages to their page variants
    type_chart: Arc<RwLock<TypeChart>>,
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
            page_table: Arc::new(Mutex::new(HashMap::with_capacity(BUFFER_SIZE as usize))),
            type_chart: Arc::new(RwLock::new(type_chart)),
        }
    }

    /// Initialize a classifier page, pin it, and return its write latch.
    pub fn create_classifier_page(&self) -> Result<FrameWLatch, BufferError> {
        self._create_page(PageVariant::Classifier)
    }

    /// Initialize a dictionary page, pin it, and return its write latch.
    pub fn create_dictionary_page(&self) -> Result<FrameWLatch, BufferError> {
        self._create_page(PageVariant::Dictionary)
    }

    /// Initialize a relation page, pin it, and return its write latch.
    pub fn create_relation_page(&self) -> Result<FrameWLatch, BufferError> {
        self._create_page(PageVariant::Relation)
    }

    /// Initialize a new page, pin it, and return its write latch.
    /// If there are no open buffer frames and all existing pages are pinned, then return an error.
    fn _create_page(&self, variant: PageVariant) -> Result<FrameWLatch, BufferError> {
        // Acquire locks for page table and type chart (in this order).
        let mut page_table = self.page_table.lock().unwrap();
        let mut type_chart = self.type_chart.write().unwrap();

        match self.replacer.evict() {
            Some(frame_id) => {
                // Acquire write latch for frame to be occupied by new page.
                let frame_arc = self.buffer.get(frame_id);
                let mut frame = frame_arc.write().unwrap();

                // Assert that selected page is a valid victim page.
                // TODO: handle pin assertions in page replacer
                frame.assert_unpinned();

                // Allocate space on disk and initialize the new page.
                let new_page_id = self.disk_manager.allocate_page();
                let new_page = init_page_variant(new_page_id, variant);

                // Update the page table and type chart.
                // If the frame contains a modified victim page, flush its data out to disk.
                if let Some(victim) = frame.get_page() {
                    if frame.is_dirty() {
                        self.disk_manager
                            .write_page(victim.get_id(), victim.as_bytes());
                    }

                    // .unwrap() ok since victim page must have an page table entry.
                    page_table.remove(&victim.get_id()).unwrap();
                }
                page_table.insert(new_page_id, frame_id);
                type_chart.insert(new_page_id, variant);

                // Place the new page in the buffer frame, flag it as dirty, and pin it.
                frame.overwrite(Some(new_page));
                frame.set_dirty_flag(true);
                frame.pin();
                self.replacer.pin(frame_id);

                // Return the write latch to the new page.
                Ok(frame)
            }
            None => Err(BufferError::NoBufFrame),
        }
    }

    /// Fetch the specified page, pin it, and return its read latch.
    /// If the page does not exist in the buffer, then fetch the page from disk.
    /// If the page does not exist on disk, then return an error.
    pub fn fetch_page_read(&self, page_id: PageIdT) -> Result<FrameRLatch, BufferError> {
        // Assert that the page exists on disk.
        if !self.disk_manager.is_allocated(page_id) {
            return Err(BufferError::PageDiskDNE);
        }

        // Acquire locks for page table and type chart (in this order).
        let mut page_table = self.page_table.lock().unwrap();
        let mut type_chart = self.type_chart.read().unwrap();

        match self.rlookup(&page_table, page_id) {
            // If the page already exists in the buffer, pin it and return its read latch.
            Some(frame) => {
                frame.pin();
                self.replacer.pin(frame.get_id());
                Ok(frame)
            }
            // Otherwise, retrieve the page from disk and (possibly) replace a page in the buffer.
            // If all frames are occupied and pinned, give up and return an error.
            None => {
                let (frame, frame_arc) = self._fetch_from_disk(page_id, page_table, type_chart)?;

                // Release the write latch, then acquire and return a read latch.
                drop(frame);
                Ok(frame_arc.read().unwrap())
            }
        }
    }

    /// Fetch the specified page, pin it, and return its write latch.
    /// If the page does not exist in the buffer, then fetch the page from disk.
    /// If the page does not exist on disk, then return an error.
    pub fn fetch_page_write(&self, page_id: PageIdT) -> Result<FrameWLatch, BufferError> {
        // Assert that the page exists on disk.
        if !self.disk_manager.is_allocated(page_id) {
            return Err(BufferError::PageDiskDNE);
        }

        // Acquire locks for page table and type chart (in this order).
        let mut page_table = self.page_table.lock().unwrap();
        let mut type_chart = self.type_chart.read().unwrap();

        match self.wlookup(&page_table, page_id) {
            // If the page already exists in the buffer, pin it and return its read latch.
            Some(frame) => {
                frame.pin();
                self.replacer.pin(frame.get_id());
                Ok(frame)
            }
            // Otherwise, retrieve the page from disk and (possibly) replace a page in the buffer.
            // If all frames are occupied and pinned, give up and return an error.
            None => {
                let (frame, _) = self._fetch_from_disk(page_id, page_table, type_chart)?;
                Ok(frame)
            }
        }
    }

    /// Fetch the specified page from disk. If there is no available frame in the buffer, return
    /// an error.
    fn _fetch_from_disk(
        &self,
        page_id: PageIdT,
        mut page_table: MutexGuard<PageTable>,
        type_chart: RwLockReadGuard<TypeChart>,
    ) -> Result<(FrameWLatch, FrameArc), BufferError> {
        match self.replacer.evict() {
            Some(frame_id) => {
                // Acquire write latch for victim page.
                let frame_arc = self.buffer.get(frame_id);
                let mut frame = frame_arc.write().unwrap();

                // Assert that selected page is a valid victim page.
                // TODO: handle pin assertions in page replacer
                frame.assert_unpinned();

                // Fetch the requested page into memory from disk.
                let mut page: Box<dyn Page + Send + Sync> = match type_chart.get(&page_id) {
                    Some(variant) => init_page_variant(page_id, *variant),
                    None => panic!("Page ID {} does not have a type chart entry", page_id),
                };
                self.disk_manager.read_page(page_id, page.as_mut_bytes());

                // Update the page table.
                // If the frame contains a modified victim page, flush its data out to disk.
                if let Some(victim) = frame.get_page() {
                    if frame.is_dirty() {
                        self.disk_manager
                            .write_page(victim.get_id(), victim.as_bytes())
                    }

                    // .unwrap() ok since victim page must have an page table entry.
                    page_table.remove(&victim.get_id()).unwrap();
                }
                page_table.insert(page_id, frame_id);

                // Place the fetched page in the buffer frame and pin it.
                frame.overwrite(Some(page));
                frame.pin();
                self.replacer.pin(frame_id);

                // Return the write latch.
                Ok((frame, frame_arc))
            }
            None => Err(BufferError::NoBufFrame),
        }
    }

    /// Delete the specified page. If the page is pinned, then return an error.
    pub fn delete_page(&self, page_id: PageIdT) -> Result<(), BufferError> {
        if !self.disk_manager.is_allocated(page_id) {
            return Err(BufferError::PageDiskDNE);
        }

        let mut page_table = self.page_table.lock().unwrap();
        let mut type_chart = self.type_chart.write().unwrap();

        match self.wlookup(&page_table, page_id) {
            Some(mut frame) => {
                match frame.get_pin_count() {
                    0 => {
                        frame.overwrite(None);

                        // .unwrap() ok since page exists in buffer.
                        page_table.remove(&page_id).unwrap();

                        // .unwrap() ok since allocated page must have type chart entry.
                        type_chart.remove(&page_id).unwrap();

                        self.disk_manager.deallocate_page(page_id);
                        Ok(())
                    }
                    _ => Err(BufferError::PagePinned),
                }
            }
            None => {
                // .unwrap() ok since allocated page must have type chart entry.
                type_chart.remove(&page_id).unwrap();

                self.disk_manager.deallocate_page(page_id);
                Ok(())
            }
        }
    }

    /// Flush the specified page to disk. Return an error if the page does not exist in the buffer.
    pub fn flush_page(&self, page_id: PageIdT) -> Result<(), BufferError> {
        let page_table = self.page_table.lock().unwrap();
        match self.rlookup(&page_table, page_id) {
            Some(frame) => {
                if frame.is_dirty() {
                    // .unwrap() ok since dirty frame implies frame contains a page.
                    let page = frame.get_page().unwrap();
                    self.disk_manager.write_page(page.get_id(), page.as_bytes())
                }
                Ok(())
            }
            None => Err(BufferError::PageBufDNE),
        }
    }

    /// Flush all pages to disk.
    pub fn flush_all_pages(&self) -> Result<(), BufferError> {
        for frame_id in 0..self.buffer.size() {
            let frame_arc = self.buffer.get(frame_id);
            let frame = frame_arc.read().unwrap();
            if frame.is_dirty() {
                // .unwrap() ok since dirty frame implies frame contains a page.
                let page = frame.get_page().unwrap();
                self.disk_manager.write_page(page.get_id(), page.as_bytes())
            }
        }
        Ok(())
    }

    /// Unpin the page contained in the specified frame and release the latch.
    pub fn unpin(&self, frame: impl FrameLatch) {
        match frame.get_page() {
            Some(_) => {
                frame.unpin();
                if frame.get_pin_count() == 0 {
                    self.replacer.unpin(frame.get_id());
                }
            }
            None => panic!("Attempted to unpin an empty buffer frame"),
        }
    }

    /// Find the specified page in the page table, and return its read latch.
    fn rlookup(&self, page_table: &MutexGuard<PageTable>, page_id: PageIdT) -> Option<FrameRLatch> {
        match page_table.get(&page_id) {
            Some(&frame_id) => {
                let frame_arc = self.buffer.get(frame_id);
                Some(frame_arc.read().unwrap())
            }
            None => None,
        }
    }

    /// Find the specified page in the page table, and return its write latch.
    fn wlookup(&self, page_table: &MutexGuard<PageTable>, page_id: PageIdT) -> Option<FrameWLatch> {
        match page_table.get(&page_id) {
            Some(&frame_id) => {
                let frame_arc = self.buffer.get(frame_id);
                Some(frame_arc.write().unwrap())
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

    /// Error to be thrown when the specified page does not exist in the buffer. Does NOT
    /// guarantee that the page exists on disk.
    PageBufDNE,

    /// Error to be thrown when the specified page does not exist on disk.
    PageDiskDNE,
}
