use crate::buffer::manager::BufferManager;
use crate::execution::transaction::Transaction;
use crate::index::{Index, IndexMeta};
use crate::relation::record::{Record, RecordId};
use std::collections::BTreeMap;

/// A database index based upon the standard library B-tree.
/// NOT to be mistaken for a B+ tree, which is what is typically used for database indexes.
pub struct BTreeIndex<'a, K: std::cmp::Ord, V> {
    meta: IndexMeta<'a>,
    tree: BTreeMap<K, V>,
}

impl<'a, K: std::cmp::Ord, V> BTreeIndex<'a, K, V> {
    pub fn new(meta: IndexMeta<'a>) -> Self {
        Self {
            meta,
            tree: BTreeMap::new(),
        }
    }
}

impl<'a, K: std::cmp::Ord, V> Index<'a> for BTreeIndex<'a, K, V> {
    fn get(key: &Record, txn: &'a Transaction) -> Vec<RecordId> {
        todo!()
    }

    fn set(key: &Record, rid: RecordId, txn: &'a Transaction) {
        todo!()
    }

    fn delete(key: &Record, rid: RecordId, txn: &'a Transaction) {
        todo!()
    }
}
