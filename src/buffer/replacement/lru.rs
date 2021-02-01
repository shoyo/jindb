/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::replacement::PageReplacer;
use crate::constants::BufferFrameIdT;
use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

type BufferFrameIdCell = Rc<RefCell<BufferFrameIdT>>;

/// An LRU eviction policy for the database buffer.
#[derive(Debug)]
pub struct LRUReplacer {
    /// A queue for maintaining buffer frame IDs. The head of the queue is always the next frame in
    /// line to be evicted.
    queue: Arc<Mutex<LinkedList<BufferFrameIdCell>>>,

    /// Mapping of a frame ID to its corresponding index in self.queue.
    /// This allows constant-time lookups for a given frame ID in the eviction queue.
    map: Arc<Mutex<HashMap<BufferFrameIdT, BufferFrameIdCell>>>,
}

impl LRUReplacer {
    pub fn new(buffer_size: BufferFrameIdT) -> Self {
        let mut queue = LinkedList::new();
        let mut map = HashMap::with_capacity(buffer_size as usize);
        for frame_id in 0..buffer_size {
            let node = Rc::new(RefCell::new(frame_id));
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
        let mut map = self.map.lock().unwrap();
        let mut queue = self.queue.lock().unwrap();

        let mut cursor = queue.cursor_front_mut();
        todo!()
    }

    fn pin(&self, frame_id: BufferFrameIdT) {
        let map = self.map.lock().unwrap();
        match map.get(&frame_id) {
            Some(_node) => {
                let queue = self.queue.lock().unwrap();
            }
            None => todo!(),
        }
        let queue = self.queue.lock().unwrap();
        todo!()
    }

    fn unpin(&self, frame_id: BufferFrameIdT) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(Rc::new(RefCell::new(frame_id)));
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore]
    #[test]
    fn test_create_lru() {
        let lru = LRUReplacer::new(5);
        lru.pin(2);
        lru.pin(1);
        lru.pin(4);
        lru.pin(0);
        assert_eq!(lru.evict().unwrap(), 3);
        assert_eq!(lru.evict().unwrap(), 2);
    }
}
