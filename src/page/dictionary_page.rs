/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{LsnT, PageIdT, DICTIONARY_PAGE_ID, PAGE_SIZE};
use crate::page::{read_str256, read_u32, write_str256, write_u32, Page};

const COUNT_OFFSET: u32 = 0;
const COUNT_LENGTH: u32 = 4;
const NAME_LENGTH: u32 = 64;
const ROOT_LENGTH: PageIdT = 4;

/// The page with ID equal to DICTIONARY_PAGE_ID is a special page designated as the "dictionary
/// page", which stores metadata for the relations and indexes in the database.
/// Specifically, it stores key value pairs of relation/index names and their corresponding root
/// page ID. It also stores the total number of entries in the page.
///
/// Whenever a relation or index is needed, the corresponding root page ID is looked up in this
/// dictionary page. The dictionary page is always pinned to the buffer pool while the
/// database is running.
///
/// Data format (number denotes size in bytes):
/// +-----------------+-------------------+--------------------------+-----+
/// | ENTRY COUNT (4) | ENTRY 1 NAME (64) | ENTRY 1 ROOT PAGE ID (4) | ... |
/// +-----------------+-------------------+--------------------------+-----+

/// Note: Although a pin count and dirty flag exist, the current implementation is such
/// that the dictionary page is never evicted from the buffer.
pub struct DictionaryPage {
    /// A unique identifier for the page (always 0)
    id: PageIdT,

    /// Raw byte array
    data: [u8; PAGE_SIZE as usize],

    /// Number of pins on the page (pinned by concurrent threads)
    pin_count: u32,

    /// True if data has been modified after reading from disk
    is_dirty: bool,
}

impl Page for DictionaryPage {
    fn get_id(&self) -> u32 {
        self.id
    }

    fn get_pin_count(&self) -> u32 {
        self.pin_count
    }

    fn incr_pin_count(&mut self) {
        self.pin_count += 1;
    }

    fn decr_pin_count(&mut self) {
        self.pin_count -= 1;
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    fn set_dirty_flag(&mut self, flag: bool) {
        self.is_dirty = flag;
    }

    fn get_lsn(&self) -> u32 {
        unimplemented!()
    }

    fn set_lsn(&mut self, lsn: LsnT) {
        unimplemented!()
    }
}

impl DictionaryPage {
    /// Construct a new dictionary page.
    pub fn new() -> Self {
        Self {
            id: DICTIONARY_PAGE_ID,
            data: [0; PAGE_SIZE as usize],
            pin_count: 0,
            is_dirty: false,
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

    /// Search the dictionary for the given entry name and return the corresponding root page ID
    /// if it exists. Return None if the entry is not found.
    pub fn get(&self, entry_name: &str) -> Option<PageIdT> {
        let count = read_u32(&self.data, COUNT_OFFSET).unwrap() as usize;
        let count_size = (COUNT_OFFSET + COUNT_LENGTH) as usize;
        let entry_size = (NAME_LENGTH + ROOT_LENGTH) as usize;

        for i in (count_size..count_size + count * entry_size).step_by(entry_size) {
            let name = read_str256(&self.data, i as u32).unwrap();
            if name == entry_name {
                let page_id = read_u32(&self.data, i as u32 + NAME_LENGTH).unwrap();
                return Some(page_id);
            }
        }
        None
    }

    /// Insert an entry name/root page ID pair into the dictionary.
    pub fn set(&mut self, name: &str, page_id: PageIdT) {
        let count = read_u32(&self.data, COUNT_OFFSET).unwrap();
        let name_offset = COUNT_OFFSET + COUNT_LENGTH + count * (NAME_LENGTH + ROOT_LENGTH);
        let root_offset = name_offset + NAME_LENGTH;

        write_u32(&mut self.data, COUNT_OFFSET, count + 1).unwrap();
        write_str256(&mut self.data, name_offset, name).unwrap();
        write_u32(&mut self.data, root_offset, page_id).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Return a dictionary page with a pre-configured byte array.
    fn setup() -> DictionaryPage {
        let mut array = [0; PAGE_SIZE as usize];
        let entries = get_entries();

        write_u32(&mut array, COUNT_OFFSET, entries.len() as u32).unwrap();
        for (i, (name, page_id)) in entries.iter().enumerate() {
            let name_offset = COUNT_OFFSET + COUNT_LENGTH + i as u32 * (NAME_LENGTH + ROOT_LENGTH);
            let root_offset = name_offset + NAME_LENGTH;

            write_str256(&mut array, name_offset, *name).unwrap();
            write_u32(&mut array, root_offset, *page_id).unwrap();
        }

        DictionaryPage {
            id: DICTIONARY_PAGE_ID,
            data: array,
            pin_count: 0,
            is_dirty: false,
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
        let page = setup();
        let id = page.get("districts");
        assert!(id.is_none())
    }

    #[test]
    fn test_get_existing_entry() {
        let page = setup();
        let entries = get_entries();
        for (name, page_id) in &entries {
            let id = page.get(*name);
            assert!(id.is_some());
            assert_eq!(id.unwrap(), *page_id);
        }
    }

    #[test]
    fn test_set_nonexisting_entry() {
        let mut page = setup();
        page.set("foo", 111);
        let offset = COUNT_OFFSET + COUNT_LENGTH + 4 * (NAME_LENGTH + ROOT_LENGTH);
        assert_eq!(read_str256(&page.data, offset).unwrap(), "foo");
        assert_eq!(read_u32(&page.data, offset + 64).unwrap(), 111);
    }

    #[test]
    #[ignore]
    /// Assert that setting an existing entry overwrites the existing page ID instead of adding
    /// a new entry.
    fn test_set_existing_entry() {
        let mut page = setup();
        let expected = 413;
        page.set("students", expected);
        let actual = page.get("students").unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    #[should_panic]
    fn test_set_entry_overflow() {
        let mut page = setup();
        let max_count = PAGE_SIZE - COUNT_OFFSET - COUNT_LENGTH / (NAME_LENGTH + ROOT_LENGTH);
        page.set_count(max_count);

        // Should panic due to overflow.
        page.set("districts", 2222);
    }
}
