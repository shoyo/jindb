/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jindb for more information about this project and its license.
 */

use crate::buffer::replacement::PageReplacer;
use crate::constants::BufferFrameIdT;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Type alias for LRU queue.
type DequeNode = Arc<BufferFrameIdT>;

/// An LRU eviction policy for the database buffer.
#[derive(Debug)]
pub struct LRUReplacer {
    /// A queue for maintaining buffer frame IDs. The head of the queue is always the next frame in
    /// line to be evicted.
    ///
    /// Note: This queue uses a vector-based implementation instead of a linked list due to
    /// limitations with Rust's std::collections::LinkedList. Technically, a linked list would
    /// allow for O(1) removal given that you have a reference to the node to be deleted. This
    /// combined with O(1) lookups via the map would allow for O(1) evict/pin/unpin operations.
    /// Performance in practice may be better with a VecDeque however, since vector-based
    /// containers are generally more memory efficient on modern CPUs. Switching the queue
    /// implementation to a custom-built linked list and benchmarking performance is a future
    /// todo.
    queue: Arc<Mutex<VecDeque<DequeNode>>>,

    /// Mapping of a frame ID to its corresponding index in self.queue.
    /// This allows constant-time lookups for a given frame ID in the eviction queue.
    map: Arc<Mutex<HashMap<BufferFrameIdT, DequeNode>>>,
}

impl LRUReplacer {
    pub fn new(buffer_size: BufferFrameIdT) -> Self {
        let mut queue = VecDeque::with_capacity(buffer_size as usize);
        let mut map = HashMap::with_capacity(buffer_size as usize);
        for frame_id in 0..buffer_size {
            let node = Arc::new(frame_id);
            queue.push_back(node.clone());
            map.insert(frame_id, node);
        }
        Self {
            queue: Arc::new(Mutex::new(queue)),
            map: Arc::new(Mutex::new(map)),
        }
    }
}

impl PageReplacer for LRUReplacer {
    fn evict(&self) -> Option<BufferFrameIdT> {
        let mut queue = self.queue.lock().unwrap();
        match queue.pop_front() {
            Some(node) => Some(*node),
            None => None,
        }
    }

    fn pin(&self, frame_id: BufferFrameIdT) {
        let map = self.map.lock().unwrap();
        match map.get(&frame_id) {
            Some(_node) => {
                let _queue = self.queue.lock().unwrap();
            }
            None => todo!(),
        }
        let _queue = self.queue.lock().unwrap();
    }

    fn unpin(&self, frame_id: BufferFrameIdT) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(Arc::new(frame_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> LRUReplacer {
        let test_buffer_size = 5;
        LRUReplacer::new(test_buffer_size)
    }

    #[test]
    fn test_create_lru() {
        let _lru = setup();
    }
}
