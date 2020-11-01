pub mod buffer_manager;
pub mod latch;

#[cfg(test)]
mod tests {
    use crate::buffer::*;
    use crate::storage::*;

    #[test]
    fn test_create_buffer_manager() {
        let dm = disk_manager::DiskManager::new();
        let bm = buffer_manager::BufferManager::new(8, dm);
        assert!(false);
    }
}
