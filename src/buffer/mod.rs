pub mod manager;

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_create_buffer_manager() {
        let dm = disk::manager::DiskManager::new();
        let _ = buffer::manager::BufferManager::new(8, dm);
        assert!(true);
    }
}
