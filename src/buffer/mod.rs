/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::BufferFrameIdT;
use crate::page::Page;

use std::any::Any;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub mod manager;
pub mod replacement;

/// The database buffer pool to be managed by the buffer manager.
pub struct Buffer {
    pool: Vec<FrameArc>,
}

impl Buffer {
    pub fn new(size: BufferFrameIdT) -> Self {
        let mut pool = Vec::with_capacity(size as usize);
        for i in 0..size {
            pool.push(Arc::new(RwLock::new(BufferFrame::new(i))));
        }
        Self { pool }
    }

    pub fn get(&self, id: BufferFrameIdT) -> FrameArc {
        self.pool[id as usize].clone()
    }

    pub fn size(&self) -> BufferFrameIdT {
        self.pool.len() as BufferFrameIdT
    }
}

/// A single buffer frame contained in a buffer pool.
/// The buffer frame maintains metadata about the contained page, such as a dirty flag and pin
/// count. The buffer ID should never change after the buffer frame has been initialized.
pub struct BufferFrame {
    /// A unique identifier for this buffer frame
    id: BufferFrameIdT,

    /// The database page contained in this buffer frame
    page: Option<Box<dyn Page + Send + Sync>>,

    /// True if the contained page has been modified since being read from disk
    dirty_flag: bool,

    /// Number of active references to the contained page
    pin_count: Arc<Mutex<u32>>,

    /// Number of times the contained page has been accessed since being read from disk
    usage_count: Arc<Mutex<u32>>,
}

impl BufferFrame {
    /// Initialize a new buffer frame.
    pub fn new(id: BufferFrameIdT) -> Self {
        Self {
            id,
            page: None,
            dirty_flag: false,
            pin_count: Arc::new(Mutex::new(0)),
            usage_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Return an immutable reference to the frame ID.
    pub fn get_id(&self) -> BufferFrameIdT {
        self.id
    }

    /// Return an immutable reference to the contained page wrapped in an Option.
    pub fn get_page(&self) -> Option<&Box<dyn Page + Send + Sync>> {
        self.page.as_ref()
    }

    /// Return a mutable reference to the contained page wrapped in an Option.
    pub fn get_mut_page(&mut self) -> Option<&mut Box<dyn Page + Send + Sync>> {
        self.page.as_mut()
    }

    /// Return the dirty flag of this buffer frame.
    pub fn is_dirty(&self) -> bool {
        self.dirty_flag
    }

    /// Set the dirty flag of this buffer frame.
    pub fn set_dirty_flag(&mut self, flag: bool) {
        self.dirty_flag = flag;
    }

    /// Return the pin count of this buffer frame.
    pub fn get_pin_count(&self) -> u32 {
        let pins = self.pin_count.lock().unwrap();
        *pins
    }

    /// Increase the pin count of this buffer frame by 1.
    pub fn pin(&self) {
        let pins = self.pin_count.lock().unwrap();
        *pins += 1;
    }

    /// Decrease the pin count of this buffer frame by 1.
    /// Panics if the pin count is 0.
    pub fn unpin(&self) {
        let pins = self.pin_count.lock().unwrap();
        if *pins == 0 {
            panic!("Cannot unpin a page with pin count equal to 0");
        }
        *pins -= 1;
    }

    /// Overwrite the existing page and reset buffer frame metadata.
    pub fn overwrite(&mut self, page: Option<Box<dyn Page + Send + Sync>>) {
        self.page = page;
        self.dirty_flag = false;
        self.pin_count = Arc::new(Mutex::new(0));
        self.usage_count = Arc::new(Mutex::new(0));
    }

    /// Panic if the buffer frame has a pin count greater than 0.
    pub fn assert_unpinned(&self) {
        let pins = self.pin_count.lock().unwrap();
        if *pins != 0 {
            panic!(
                "Frame ID: {} contains a page that is pinned ({} pins)",
                self.id, *pins
            )
        }
    }
}

/// Type alias for a guarded buffer frame.
pub type FrameArc = Arc<RwLock<BufferFrame>>;
pub type FrameRLatch<'a> = RwLockReadGuard<'a, BufferFrame>;
pub type FrameWLatch<'a> = RwLockWriteGuard<'a, BufferFrame>;

trait FrameLatch {
    fn get_id(&self) -> BufferFrameIdT;
    fn get_page(&self) -> Option<&Box<dyn Page + Send + Sync>>;
    fn get_pin_count(&self) -> u32;
    fn unpin(&self);
}

impl<'a> FrameLatch for FrameRLatch<'a> {
    fn get_id(&self) -> u32 {
        self.get_id()
    }

    fn get_page(&self) -> Option<&Box<dyn Page + Send + Sync>> {
        self.get_page()
    }

    fn get_pin_count(&self) -> u32 {
        self.get_pin_count()
    }

    fn unpin(&self) {
        self.unpin()
    }
}

impl<'a> FrameLatch for FrameWLatch<'a> {
    fn get_id(&self) -> u32 {
        self.get_id()
    }

    fn get_page(&self) -> Option<&Box<dyn Page + Send + Sync>> {
        self.get_page()
    }

    fn get_pin_count(&self) -> u32 {
        self.get_pin_count()
    }

    fn unpin(&self) {
        self.unpin()
    }
}
