/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::BufferFrameIdT;
use crate::page::Page;

use std::sync::{Arc, RwLock};

pub mod manager;
pub mod replacement;

/// The database buffer pool to be managed by the buffer manager.
pub struct Buffer {
    pool: Vec<FrameLatch>,
}

impl Buffer {
    pub fn new(size: BufferFrameIdT) -> Self {
        let mut pool = Vec::with_capacity(size as usize);
        for i in 0..size {
            pool.push(Arc::new(RwLock::new(BufferFrame::new(i))));
        }
        Self { pool }
    }

    pub fn get(&self, id: BufferFrameIdT) -> FrameLatch {
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
    pin_count: u32,

    /// Number of times the contained page has been accessed since being read from disk
    usage_count: u32,
}

impl BufferFrame {
    /// Initialize a new buffer frame.
    pub fn new(id: BufferFrameIdT) -> Self {
        Self {
            id,
            page: None,
            dirty_flag: false,
            pin_count: 0,
            usage_count: 0,
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

    /// Overwrite the existing page.
    pub fn overwrite(&mut self, page: Option<Box<dyn Page + Send + Sync>>) {
        self.page = page;
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
        self.pin_count
    }

    /// Increase the pin count of this buffer frame by 1.
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    /// Decrease the pin count of this buffer frame by 1.
    /// Panics if the pin count is 0.
    pub fn unpin(&mut self) {
        if self.pin_count == 0 {
            panic!("Cannot unpin a page with pin count equal to 0");
        }
        self.pin_count -= 1;
    }

    /// Reset this buffer frame to an initial, empty state.
    /// This method is typically called when a database page is removed from this buffer frame.
    pub fn reset(&mut self) {
        self.page = None;
        self.dirty_flag = false;
        self.pin_count = 0;
        self.usage_count = 0;
    }

    /// Panic if the buffer frame has a pin count greater than 0.
    pub fn assert_no_pins(&self) {
        if self.pin_count != 0 {
            panic!(
                "Frame ID: {} contains a page that is pinned ({} pins)",
                self.id, self.pin_count
            )
        }
    }
}

/// Type alias for a guarded buffer frame.
pub type FrameLatch = Arc<RwLock<BufferFrame>>;
