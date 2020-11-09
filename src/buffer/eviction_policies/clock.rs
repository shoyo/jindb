use super::policy::Policy;
use crate::common::constants::BufferFrameIdT;

/// A clock eviction policy for the database buffer.
struct ClockPolicy {}

impl Policy for ClockPolicy {
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
