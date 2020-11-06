pub mod buffer_manager;

#[cfg(test)]
mod tests {
    use crate::buffer::*;
    use crate::disk::*;

    #[test]
    fn test_create_buffer_manager() {
        let dm = disk_manager::DiskManager::new();
        let _ = buffer_manager::BufferManager::new(8, dm);
        assert!(true);
    }
}
