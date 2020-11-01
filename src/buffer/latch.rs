use std::sync::Mutex;

/// A latch to control read/write access to a given block in memory.
pub struct Latch {
    mutex: Mutex<()>,
}

impl Latch {
    pub fn new() -> Self {
        Self {
            mutex: Mutex::new(()),
        }
    }
}
