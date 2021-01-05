/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::replacement::PageReplacer;
use crate::common::BufferFrameIdT;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// A terribly inefficient eviction policy with O(1) evict and O(N) pin/unpin operations. This
/// struct is strictly meant as a placeholder policy.
/// Use a LRU or clock based eviction policy during actual database use.
pub struct SlowReplacer {
    queue: Arc<Mutex<VecDeque<BufferFrameIdT>>>,
}

impl SlowReplacer {
    pub fn new(buffer_size: BufferFrameIdT) -> Self {
        let mut queue = VecDeque::with_capacity(buffer_size as usize);
        for frame_id in 0..buffer_size {
            queue.push_back(frame_id);
        }
        Self {
            queue: Arc::new(Mutex::new(queue)),
        }
    }
}

impl PageReplacer for SlowReplacer {
    fn evict(&self) -> Option<u32> {
        let mut queue = self.queue.lock().unwrap();
        queue.pop_front()
    }

    fn pin(&self, frame_id: u32) {
        let mut queue = self.queue.lock().unwrap();

        let mut count = 0;
        let mut idx = 0;

        for i in 0..queue.len() {
            if queue[i] == frame_id {
                count += 1;
                idx = i;
            }
        }

        if count > 1 {
            panic!(
                "Found {} instances of frame ID {} in queue, expected 0 or 1",
                count, frame_id
            );
        }
        if count == 1 {
            queue.remove(idx);
        }
        // If the page has already been pinned, count will equal 0 and we do nothing.
    }

    fn unpin(&self, frame_id: u32) {
        let mut queue = self.queue.lock().unwrap();
        let count = queue
            .iter()
            .map(|id| id == &frame_id)
            .collect::<Vec<bool>>()
            .len();
        if count > 0 {
            panic!(
                "Found {} instances of frame ID {} in queue, expected 0",
                count, frame_id
            );
        }
        queue.push_back(frame_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evict() {
        let test_buffer_size = 5;
        let policy = SlowReplacer::new(test_buffer_size);

        for i in 0..test_buffer_size {
            let id = policy.evict();
            assert!(id.is_some());
            assert_eq!(id.unwrap(), i);
        }
        assert!(policy.evict().is_none())
    }
}
