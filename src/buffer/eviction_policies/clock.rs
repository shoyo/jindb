/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::eviction_policies::EvictionPolicy;
use crate::common::BufferFrameIdT;

/// A clock eviction policy for the database buffer.
pub struct ClockPolicy {}

impl ClockPolicy {
    pub fn new() -> Self {
        Self {}
    }
}

impl EvictionPolicy for ClockPolicy {
    fn evict(&mut self) -> Option<BufferFrameIdT> {
        todo!()
    }

    fn pin(&mut self, frame_id: BufferFrameIdT) {
        todo!()
    }

    fn unpin(&mut self, frame_id: BufferFrameIdT) {
        todo!()
    }
}
