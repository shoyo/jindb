pub mod block;
pub mod constants;
pub mod disk_manager;
pub mod record;
pub mod relation;
pub mod schema;

/// A serialized representation of a database block meant for
/// testing and debugging.

struct BlockSnapshot {
    block_id: u32,
    free_space_pointer: u32,
    num_records: u32,
    free_space_remaining: u32,
    record_pointers: Vec<(u32, u32)>,
    records: Vec<Vec<u8>>,
}

impl BlockSnapshot {
    pub fn new(block: &block::Block) -> Self {
        let id = block.get_block_id().unwrap();
        let ptr = block.get_free_space_pointer().unwrap();
        let num = block.get_num_records().unwrap();
        let space = block.get_free_space_remaining();

        let ptrs = Vec::new();
        let records = Vec::new();

        Self {
            block_id: id,
            free_space_pointer: ptr,
            num_records: num,
            free_space_remaining: space,
            record_pointers: ptrs,
            records: records,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_block() {
        let block = block::Block::new(123);
        let block_repr = BlockSnapshot::new(&block);
        assert_eq!(block_repr.block_id, 123);
        assert_eq!(block_repr.free_space_pointer, constants::BLOCK_SIZE - 1);
        assert_eq!(block_repr.num_records, 0);
        assert_eq!(
            block_repr.free_space_remaining,
            constants::BLOCK_SIZE - constants::RECORDS_OFFSET
        );
    }

    #[test]
    fn test_write_block() {
        let block = block::Block::new(123);
        let dm = disk_manager::DiskManager::new();
    }

    #[test]
    fn test_read_block() {
        let block = block::Block::new(123);
        let dm = disk_manager::DiskManager::new();
    }
}
