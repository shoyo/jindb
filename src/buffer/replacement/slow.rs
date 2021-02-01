/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::replacement::PageReplacer;
use crate::constants::BufferFrameIdT;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

/// A terribly inefficient eviction policy with O(1) evict and O(N) pin/unpin operations. This
/// struct is strictly meant as a placeholder policy.
/// Use a LRU or clock based eviction policy during actual database use.
pub struct SlowReplacer {
    queue: Arc<Mutex<VecDeque<BufferFrameIdT>>>,
    set: Arc<Mutex<HashSet<BufferFrameIdT>>>,
}

impl SlowReplacer {
    pub fn new(buffer_size: BufferFrameIdT) -> Self {
        let mut queue = VecDeque::with_capacity(buffer_size as usize);
        let mut set = HashSet::with_capacity(buffer_size as usize);
        for frame_id in 0..buffer_size {
            queue.push_back(frame_id);
            set.insert(frame_id);
        }
        Self {
            queue: Arc::new(Mutex::new(queue)),
            set: Arc::new(Mutex::new(set)),
        }
    }
}

impl PageReplacer for SlowReplacer {
    fn evict(&self) -> Option<u32> {
        let mut queue = self.queue.lock().unwrap();
        let mut set = self.set.lock().unwrap();

        match queue.pop_front() {
            Some(frame_id) => {
                assert!(set.remove(&frame_id));
                Some(frame_id)
            }
            None => None,
        }
    }

    fn pin(&self, frame_id: u32) {
        let mut queue = self.queue.lock().unwrap();
        let mut set = self.set.lock().unwrap();

        // If `frame_id` has already been evicted or pinned, it does not exist in the set and and
        // the following operation is a no-op.
        // If `frame_id` exists in the set, it is removed from both the set and queue.
        if set.remove(&frame_id) {
            let matches = queue
                .iter()
                .enumerate()
                .filter(|(i, &id)| id == frame_id)
                .collect::<Vec<(usize, &BufferFrameIdT)>>();
            match matches.len() {
                0 => panic!("Frame ID {} exists in the set but not the queue"),
                1 => {
                    let idx = matches[0].0;
                    queue.remove(idx);
                }
                _ => panic!(
                    "Found {} instances of frame ID {} in queue, expected 0 or 1",
                    matches.len(),
                    frame_id
                ),
            }
        }
    }

    fn unpin(&self, frame_id: u32) {
        let mut queue = self.queue.lock().unwrap();
        let mut set = self.set.lock().unwrap();

        // If `frame_id` did not exist in the set, then it is inserted into both the set and queue.
        // If `frame_id` already existed in the set, then set.insert() returns false and the
        // following operation is a no-op.
        if set.insert(frame_id) {
            queue.push_back(frame_id);
        }
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
