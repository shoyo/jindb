/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::io::{read_str256, read_u32, write_str256, write_u32};
use crate::common::{LsnT, PageIdT, DICTIONARY_PAGE_ID, PAGE_SIZE};
use crate::page::{Page, PageVariant};

const COUNT_OFFSET: u32 = 0;
const COUNT_LENGTH: u32 = 4;
const NAME_LENGTH: u32 = 64;
const ROOT_LENGTH: PageIdT = 4;

/// The page with ID equal to DICTIONARY_PAGE_ID is a special page designated as the "dictionary
/// page", which stores metadata for the relations and indexes in the database.
/// Specifically, it stores key-value pairs of relation/index names and their corresponding root
/// page ID. It also stores the total number of entries in the page.
///
/// Whenever a relation or index is needed, the corresponding root page ID is looked up in the
/// dictionary page.
///
/// Data format (number denotes size in bytes):
/// +-----------------+-------------------+--------------------------+-----+
/// | ENTRY COUNT (4) | ENTRY 1 NAME (64) | ENTRY 1 ROOT PAGE ID (4) | ... |
/// +-----------------+-------------------+--------------------------+-----+

pub struct DictionaryPage {
    /// A unique identifier for the page
    id: PageIdT,

    /// Raw byte array
    data: [u8; PAGE_SIZE as usize],
}

impl Page for DictionaryPage {
    fn get_id(&self) -> u32 {
        self.id
    }

    fn as_bytes(&self) -> &[u8; PAGE_SIZE as usize] {
        &self.data
    }

    fn get_mut_data(&mut self) -> &mut [u8; PAGE_SIZE as usize] {
        &mut self.data
    }

    fn get_lsn(&self) -> u32 {
        unimplemented!()
    }

    fn set_lsn(&mut self, _lsn: LsnT) {
        unimplemented!()
    }

    fn get_variant(&self) -> PageVariant {
        PageVariant::Dictionary
    }
}

impl DictionaryPage {
    /// Construct a new dictionary page.
    pub fn new(_page_id: PageIdT) -> Self {
        Self {
            id: DICTIONARY_PAGE_ID,
            data: [0; PAGE_SIZE as usize],
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
        let count = self.get_count() as usize;
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
        let count = self.get_count();
        let name_offset = COUNT_OFFSET + COUNT_LENGTH + count * (NAME_LENGTH + ROOT_LENGTH);
        let root_offset = name_offset + NAME_LENGTH;

        write_u32(&mut self.data, COUNT_OFFSET, count + 1).unwrap();
        write_str256(&mut self.data, name_offset, name).unwrap();
        write_u32(&mut self.data, root_offset, page_id).unwrap();
    }
}

impl IntoIterator for DictionaryPage {
    type Item = (String, PageIdT);
    type IntoIter = DictionaryPageIterator;

    fn into_iter(self) -> Self::IntoIter {
        DictionaryPageIterator::new(self)
    }
}

/// A supplementary struct for iterating through key/value pairs in the dictionary page.
pub struct DictionaryPageIterator {
    idx: u32,
    count: u32,
    page: DictionaryPage,
}

impl DictionaryPageIterator {
    fn new(page: DictionaryPage) -> Self {
        Self {
            idx: 0,
            count: page.get_count(),
            page,
        }
    }
}

impl Iterator for DictionaryPageIterator {
    type Item = (String, PageIdT);

    fn next(&mut self) -> Option<Self::Item> {
        let name_offset = COUNT_OFFSET + COUNT_LENGTH + self.idx * (NAME_LENGTH + ROOT_LENGTH);
        let root_offset = name_offset + NAME_LENGTH;
        let name = read_str256(self.page.as_bytes(), name_offset).unwrap();
        let root = read_u32(self.page.as_bytes(), root_offset).unwrap();

        self.idx += 1;
        if self.idx > self.count {
            return None;
        }

        Some((name, root))
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

    #[test]
    fn test_iteration() {
        let page = setup();
        let entries = get_entries();
        let mut cnt = 0;
        for (name, root) in page {
            cnt += 1;
            assert_eq!(entries.get(name.as_str()).unwrap(), &root);
        }
        assert_eq!(cnt, entries.len());
    }
}
