/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::replacement::PageReplacer;
use crate::constants::BufferFrameIdT;

/// A clock eviction policy for the database buffer.
pub struct ClockReplacer {}

impl ClockReplacer {
    pub fn new(_buffer_size: BufferFrameIdT) -> Self {
        Self {}
    }
}

impl PageReplacer for ClockReplacer {
    fn evict(&self) -> Option<BufferFrameIdT> {
        todo!()
    }

    fn pin(&self, _frame_id: BufferFrameIdT) {
        todo!()
    }

    fn unpin(&self, _frame_id: BufferFrameIdT) {
        todo!()
    }
}
