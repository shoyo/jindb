use super::policy::Policy;
use crate::common::constants::BufferFrameIdT;

/// An LRU eviction policy for the database buffer.
struct LRUPolicy {}

impl Policy for LRUPolicy {
    fn new() -> Self {
        Self {}
    }

    fn evict() -> Result<BufferFrameIdT, String> {
        todo!()
    }

    fn pin(frame_id: BufferFrameIdT) {
        todo!()
    }

    fn unpin(frame_id: BufferFrameIdT) {
        todo!()
    }
}
