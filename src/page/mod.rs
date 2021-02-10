/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::constants::{LsnT, PageIdT, PAGE_SIZE};
use crate::page::dictionary_page::DictionaryPage;
use crate::page::relation_page::RelationPage;
use std::any::Any;

pub mod dictionary_page;
pub mod relation_page;

/// A type alias for a raw byte array representing a page on disk. Lower layers of the database
/// (buffer manager, disk manager, etc.) handle pages as raw pages, and upper layers of the
/// database adapt raw pages into concrete page types (a type that implements the Page trait) as
/// needed.
pub type PageBytes = [u8; PAGE_SIZE as usize];

/// A trait for pages stored in the database. A page, regardless of its variant, is
/// constants::PAGE_SIZE bytes in length.
/// Pages can store various things, such as metadata (dictionary page), relation data (relation
/// pages), index headers (index header pages) and indexes (index pages).
pub trait Page {
    /// Create a new concrete page instance.
    fn new(bytes: PageBytes) -> Self;

    /// Return the unique page descriptor.
    fn get_id(&self) -> PageIdT;

    /// Return a reference to the raw byte array.
    fn as_bytes(&self) -> &PageBytes;

    /// Return a mutable reference to the raw byte array.
    fn as_mut_bytes(&mut self) -> &mut PageBytes;

    /// Return the log sequence number.
    fn get_lsn(&self) -> LsnT;

    /// Set the log sequence number.
    fn set_lsn(&mut self, lsn: LsnT);

    /// Return the amount of free space (in bytes) left in the page.
    fn get_free_space(&self) -> u32;

    /// Return an immutable reference to this page that implements Any.
    /// Used when downcasting to a concrete page type.
    fn as_any(&self) -> &dyn Any;

    /// Return a mutable reference to this page that implements Any.
    /// Used when downcasting to a concrete page type.
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

/// Custom errors to be used by pages.
#[derive(Debug)]
pub enum PageError {
    /// Error to be thrown when a page insertion/update would trigger an overflow.
    PageOverflow,

    /// Error to be thrown when a slot index is out of bounds.
    SlotOutOfBounds,

    /// Error to be thrown when a specified record has already been deleted and a
    /// read/update/delete operation cannot proceed.
    RecordDeleted,
}
