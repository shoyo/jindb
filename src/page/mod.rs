/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{LsnT, PageIdT, PAGE_SIZE};
use crate::page::classifier_page::ClassifierPage;
use crate::page::dictionary_page::DictionaryPage;
use crate::page::relation_page::RelationPage;

pub mod classifier_page;
pub mod dictionary_page;
pub mod relation_page;

/// A trait for pages stored in the database. A page, regardless of its variant, is
/// common::PAGE_SIZE bytes in length.
/// Pages can store various things, such as metadata (dictionary page), relation data (relation
/// pages), index headers (index header pages) and indexes (index pages).
pub trait Page {
    /// Return the unique page descriptor.
    fn get_id(&self) -> PageIdT;

    /// Return a reference to the raw byte array.
    fn as_bytes(&self) -> &[u8; PAGE_SIZE as usize];

    /// Return a mutable reference to the raw byte array.
    fn as_mut_bytes(&mut self) -> &mut [u8; PAGE_SIZE as usize];

    /// Return the log sequence number.
    fn get_lsn(&self) -> LsnT;

    /// Set the log sequence number.
    fn set_lsn(&mut self, lsn: LsnT);

    /// Return the page variant.
    fn get_variant(&self) -> PageVariant;

    /// Return the amount of free space (in bytes) left in the page.
    fn get_free_space(&self) -> u32;
}

/// Page variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageVariant {
    Classifier,
    Dictionary,
    Relation,
}

/// Initialize a boxed page instance for the given variant.
pub fn init_page_variant(page_id: PageIdT, variant: PageVariant) -> Box<dyn Page + Send + Sync> {
    match variant {
        PageVariant::Classifier => Box::new(ClassifierPage::new(page_id)),
        PageVariant::Dictionary => Box::new(DictionaryPage::new(page_id)),
        PageVariant::Relation => Box::new(RelationPage::new(page_id)),
    }
}
