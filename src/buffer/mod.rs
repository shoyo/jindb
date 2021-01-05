/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{BufferFrameIdT, PageIdT};
use crate::page::{Page, PageVariant};
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, Mutex, RwLock};

pub mod manager;
pub mod replacement;

/// The database buffer pool to be managed by the buffer manager.
pub struct Buffer {
    pool: Vec<Arc<RwLock<BufferFrame>>>,
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
}

/// A single buffer frame contained in a buffer pool.
/// The buffer frame maintains metadata about the contained page, such as a dirty flag and pin
/// count.
pub struct BufferFrame {
    /// A unique identifier for this buffer frame
    pub id: BufferFrameIdT,

    /// The database page contained in this buffer frame
    pub page: Option<Box<dyn Page + Send + Sync>>,

    /// True if the contained page has been modified since being read from disk
    pub is_dirty: bool,

    /// Number of active references to the contained page
    pub pin_count: u32,

    /// Number of times the contained page has been accessed since being read from disk
    pub usage_count: u32,
}

impl BufferFrame {
    pub fn new(id: BufferFrameIdT) -> Self {
        Self {
            id,
            page: None,
            is_dirty: false,
            pin_count: 0,
            usage_count: 0,
        }
    }
}

/// Type alias for a guarded buffer frame.
pub type FrameLatch = Arc<RwLock<BufferFrame>>;
