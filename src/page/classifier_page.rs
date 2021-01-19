/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::io::{read_u32, write_u32};
use crate::common::{PageIdT, CLASSIFIER_PAGE_ID, PAGE_SIZE};
use crate::page::{Page, PageVariant};
use std::any::Any;

/// Constants for encoding page variants as an unsigned integer.
const CLASSIFIER_TYPE: u32 = 0;
const DICTIONARY_TYPE: u32 = 1;
const RELATION_TYPE: u32 = 2;

/// Constants for byte array offsets.
const COUNT_OFFSET: u32 = 0;
const COUNT_LENGTH: u32 = 4;
const ID_LENGTH: u32 = 4;
const TYPE_LENGTH: u32 = 4;

/// The page with ID equal to CLASSIFIER_PAGE_ID is a special page designated as the "classifier
/// page", which stores metadata for other pages stored in the database.
/// Specifically, it stores key-value pairs of page IDs and page variants (relation page,
/// index page, etc.)
///
/// Page data on disk is simply an array of bytes, and contains no simple way of telling
/// what type of page it is. Whenever a page's data is read into memory, the page's variant is
/// looked up in the buffer manager's "type chart", and the corresponding page variant is
/// initialized.
///
/// The information contained in the classifier page is initialized as an in-memory hashmap
/// (referred to as the "type chart") upon database startup. Since page IDs are rarely reassigned
/// on disk, the type chart is generally append-only. The type chart is periodically flushed out
/// to the classifier page on disk with a background process.
///
/// Data format (number denotes size in bytes):
/// +-----------------+---------------+-----------------+---------------+-----------------+-----+
/// | ENTRY COUNT (4) | PAGE 1 ID (4) | PAGE 1 TYPE (4) | PAGE 2 ID (4) | PAGE 2 TYPE (4) | ... |
/// +-----------------+---------------+-----------------+---------------+-----------------+-----+

pub struct ClassifierPage {
    /// A unique identifier for the page
    id: PageIdT,

    /// Raw byte array
    data: [u8; PAGE_SIZE as usize],
}

impl Page for ClassifierPage {
    fn get_id(&self) -> u32 {
        self.id
    }

    fn as_bytes(&self) -> &[u8; PAGE_SIZE as usize] {
        &self.data
    }

    fn as_mut_bytes(&mut self) -> &mut [u8; PAGE_SIZE as usize] {
        &mut self.data
    }

    fn get_lsn(&self) -> u32 {
        unimplemented!()
    }

    fn set_lsn(&mut self, _lsn: u32) {
        unimplemented!()
    }

    fn get_free_space(&self) -> u32 {
        unimplemented!()
    }

    fn get_variant(&self) -> PageVariant {
        PageVariant::Classifier
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

impl ClassifierPage {
    /// Construct a new classifier page.
    pub fn new(_page_id: PageIdT) -> Self {
        Self {
            id: CLASSIFIER_PAGE_ID,
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
}

impl IntoIterator for ClassifierPage {
    type Item = (PageIdT, PageVariant);
    type IntoIter = ClassifierPageIterator;

    fn into_iter(self) -> Self::IntoIter {
        ClassifierPageIterator::new(self)
    }
}

pub struct ClassifierPageIterator {
    idx: u32,
    count: u32,
    page: ClassifierPage,
}

impl ClassifierPageIterator {
    fn new(page: ClassifierPage) -> Self {
        Self {
            idx: 0,
            count: page.get_count(),
            page,
        }
    }
}

impl Iterator for ClassifierPageIterator {
    type Item = (PageIdT, PageVariant);

    fn next(&mut self) -> Option<Self::Item> {
        let id_offset = COUNT_OFFSET + COUNT_LENGTH + self.idx * (ID_LENGTH + TYPE_LENGTH);
        let type_offset = id_offset + ID_LENGTH;
        let page_id = read_u32(self.page.as_bytes(), id_offset).unwrap();
        let page_type = match read_u32(self.page.as_bytes(), type_offset).unwrap() {
            CLASSIFIER_TYPE => PageVariant::Classifier,
            DICTIONARY_TYPE => PageVariant::Dictionary,
            RELATION_TYPE => PageVariant::Relation,
            unrecognized => panic!(
                "An invalid page variant integer '{}' stored in classifier for page ID: {}",
                unrecognized, page_id
            ),
        };

        self.idx += 1;
        if self.idx > self.count {
            return None;
        }

        Some((page_id, page_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Return a classifier page with a pre-configured byte array.
    fn setup() -> ClassifierPage {
        let mut array = [0; PAGE_SIZE as usize];
        let entries = get_entries();

        write_u32(&mut array, COUNT_OFFSET, entries.len() as u32).unwrap();
        for (i, (page_id, page_type)) in entries.iter().enumerate() {
            let id_offset = COUNT_OFFSET + COUNT_LENGTH + i as u32 * (ID_LENGTH + TYPE_LENGTH);
            let type_offset = id_offset + ID_LENGTH;

            write_u32(&mut array, id_offset, *page_id).unwrap();

            let page_type = match *page_type {
                PageVariant::Classifier => CLASSIFIER_TYPE,
                PageVariant::Dictionary => DICTIONARY_TYPE,
                PageVariant::Relation => RELATION_TYPE,
            };
            write_u32(&mut array, type_offset, page_type).unwrap();
        }

        ClassifierPage {
            id: CLASSIFIER_PAGE_ID,
            data: array,
        }
    }

    fn get_entries() -> HashMap<PageIdT, PageVariant> {
        let mut entries = HashMap::new();
        entries.insert(0, PageVariant::Dictionary);
        entries.insert(1, PageVariant::Classifier);
        entries.insert(1234, PageVariant::Relation);
        entries.insert(4321, PageVariant::Relation);
        entries
    }

    #[test]
    fn test_iteration() {
        let page = setup();
        let entries = get_entries();
        let mut cnt = 0;
        for (page_id, page_type) in page {
            cnt += 1;
            assert_eq!(entries.get(&page_id).unwrap(), &page_type);
        }
        assert_eq!(cnt, entries.len());
    }
}
