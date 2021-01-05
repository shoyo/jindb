/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{PageIdT, CLASSIFIER_PAGE_ID, PAGE_SIZE};
use crate::page::{read_u32, write_u32, Page, PageVariant};

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

    /// Number of pins on the page (pinned by concurrent threads)
    pin_count: u32,

    /// True if data has been modified after reading the disk
    is_dirty: bool,
}

impl Page for ClassifierPage {
    fn get_id(&self) -> u32 {
        self.id
    }

    fn get_data(&self) -> &[u8; PAGE_SIZE as usize] {
        &self.data
    }

    fn get_data_mut(&mut self) -> &mut [u8; PAGE_SIZE as usize] {
        &mut self.data
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

    fn set_lsn(&mut self, _lsn: u32) {
        unimplemented!()
    }
}

impl ClassifierPage {
    /// Construct a new classifier page.
    pub fn new() -> Self {
        Self {
            id: CLASSIFIER_PAGE_ID,
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
        let page_id = read_u32(self.page.get_data(), id_offset).unwrap();
        let page_type = match read_u32(self.page.get_data(), type_offset).unwrap() {
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
