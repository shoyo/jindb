/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::block::{read_str256, read_u32, write_str256, write_u32};
use crate::common::{BlockIdT, BLOCK_SIZE, DICTIONARY_BLOCK_ID};

const COUNT_OFFSET: u32 = 0;
const COUNT_LENGTH: u32 = 4;
const NAME_LENGTH: u32 = 64;
const ROOT_LENGTH: BlockIdT = 4;

/// The block with ID equal to 0 is a special block designated as the "dictionary block", which
/// stores metadata for the relations and indexes in the database.
/// Specifically, it stores key value pairs of relation/index names and their corresponding root
/// block ID. It also stores the total number of entries in the block.
///
/// Whenever a relation or index is needed, the corresponding root block ID is looked up in this
/// dictionary block. The dictionary block is always pinned to the buffer pool while the
/// database is running.
///
/// Data format (number denotes size in bytes):
/// +-----------------+-------------------+---------------------------+-----+
/// | ENTRY COUNT (4) | ENTRY 1 NAME (64) | ENTRY 1 ROOT BLOCK ID (4) | ... |
/// +-----------------+-------------------+---------------------------+-----+

/// Note: Pin count and dirty flag is unnecessary since the dictionary block is never evicted
/// from the buffer.
pub struct DictionaryBlock {
    /// A unique identifier for the block (always 0)
    pub id: BlockIdT,

    /// Raw byte array
    pub data: [u8; BLOCK_SIZE as usize],
}

impl DictionaryBlock {
    /// Construct a new dictionary block.
    pub fn new() -> Self {
        Self {
            id: DICTIONARY_BLOCK_ID,
            data: [0; BLOCK_SIZE as usize],
        }
    }

    /// Return the number of stored entries.
    pub fn get_count(&self) -> u32 {
        read_u32(&self.data, COUNT_OFFSET).unwrap()
    }

    /// Set the number of stored entries.
    pub fn set_count(&mut self, count: u32) {
        write_u32(&mut self.data, COUNT_OFFSET, count).unwrap()
    }

    /// Search the dictionary for the given entry name and return the corresponding root block ID
    /// if it exists. Return None if the entry is not found.
    pub fn get(&self, entry_name: &str) -> Option<BlockIdT> {
        let count = read_u32(&self.data, COUNT_OFFSET).unwrap() as usize;
        let count_size = (COUNT_OFFSET + COUNT_LENGTH) as usize;
        let entry_size = (NAME_LENGTH + ROOT_LENGTH) as usize;

        for i in (count_size..count_size + count * entry_size).step_by(entry_size) {
            let name = read_str256(&self.data, i as u32).unwrap();
            if name == entry_name {
                let block_id = read_u32(&self.data, i as u32 + NAME_LENGTH).unwrap();
                return Some(block_id);
            }
        }
        None
    }

    /// Insert an entry name/root block ID pair into the dictionary.
    pub fn set(&mut self, name: &str, block_id: BlockIdT) {
        let count = read_u32(&self.data, COUNT_OFFSET).unwrap();
        let name_offset = COUNT_OFFSET + COUNT_LENGTH + count * (NAME_LENGTH + ROOT_LENGTH);
        let root_offset = name_offset + NAME_LENGTH;

        write_u32(&mut self.data, COUNT_OFFSET, count + 1).unwrap();
        write_str256(&mut self.data, name_offset, name).unwrap();
        write_u32(&mut self.data, root_offset, block_id).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Return a dictionary block with a pre-configured byte array.
    fn setup() -> DictionaryBlock {
        let mut array = [0; BLOCK_SIZE as usize];
        let entries = get_entries();

        write_u32(&mut array, COUNT_OFFSET, entries.len() as u32).unwrap();
        for (i, (name, block_id)) in entries.iter().enumerate() {
            let name_offset = COUNT_OFFSET + COUNT_LENGTH + i as u32 * (NAME_LENGTH + ROOT_LENGTH);
            let root_offset = name_offset + NAME_LENGTH;

            write_str256(&mut array, name_offset, *name).unwrap();
            write_u32(&mut array, root_offset, *block_id).unwrap();
        }

        DictionaryBlock {
            id: DICTIONARY_BLOCK_ID,
            data: array,
        }
    }

    fn get_entries() -> HashMap<&'static str, u32> {
        let mut entries = HashMap::new();
        entries.insert("students", 314);
        entries.insert("teachers", 271);
        entries.insert("classes", 141);
        entries.insert("schools", 161);
        entries
    }

    #[test]
    fn test_get_nonexisting_entry() {
        let block = setup();
        let id = block.get("districts");
        assert!(id.is_none())
    }

    #[test]
    fn test_get_existing_entry() {
        let block = setup();
        let entries = get_entries();
        for (name, block_id) in &entries {
            let id = block.get(*name);
            assert!(id.is_some());
            assert_eq!(id.unwrap(), *block_id);
        }
    }

    #[test]
    fn test_set_nonexisting_entry() {
        let mut block = setup();
        block.set("foo", 111);
        let offset = COUNT_OFFSET + COUNT_LENGTH + 4 * (NAME_LENGTH + ROOT_LENGTH);
        assert_eq!(read_str256(&block.data, offset).unwrap(), "foo");
        assert_eq!(read_u32(&block.data, offset + 64).unwrap(), 111);
    }

    #[test]
    #[ignore]
    /// Assert that setting an existing entry overwrites the existing block ID instead of adding
    /// a new entry.
    fn test_set_existing_entry() {
        let mut block = setup();
        let expected = 413;
        block.set("students", expected);
        let actual = block.get("students").unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    #[should_panic]
    fn test_set_entry_overflow() {
        let mut block = setup();
        let max_count = BLOCK_SIZE - COUNT_OFFSET - COUNT_LENGTH / (NAME_LENGTH + ROOT_LENGTH);
        block.set_count(max_count);

        // Should panic due to overflow.
        block.set("districts", 2222);
    }
}
