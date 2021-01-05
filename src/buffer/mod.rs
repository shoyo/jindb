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

/// Type alias for a page protected by a R/W latch for concurrent access.
pub type PageLatch = Arc<RwLock<Option<Box<dyn Page + Send + Sync>>>>;

/// The database buffer and associated data structures.
/// Functions should be wary of the order in which they lock the buffer's data structures to
/// prevent deadlocks.
pub struct Buffer {
    pool: Vec<PageLatch>,
}

impl Buffer {
    pub fn new(size: BufferFrameIdT) -> Self {
        let mut pool = Vec::with_capacity(size as usize);
        for _ in 0..size {
            pool.push(Arc::new(RwLock::new(None)));
        }
        Self { pool }
    }
}
