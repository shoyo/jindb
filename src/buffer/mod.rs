/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

pub mod eviction_policies;
pub mod manager;

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_create_buffer_manager() {
        let dm = disk::manager::DiskManager::new();
        let _ = buffer::manager::BufferManager::new(dm);
        assert!(true);
    }
}
