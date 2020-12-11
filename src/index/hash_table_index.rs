/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::execution::transaction::Transaction;
use crate::index::{Index, IndexMeta};
use crate::relation::record::{Record, RecordId};
use std::collections::HashMap;

pub struct HashIndex<'a, K, V> {
    meta: IndexMeta<'a>,
    map: HashMap<K, V>,
}

impl<'a, K, V> HashIndex<'a, K, V> {
    pub fn new(meta: IndexMeta<'a>) -> Self {
        Self {
            meta,
            map: HashMap::new(),
        }
    }
}

impl<'a, K, V> Index<'a> for HashIndex<'a, K, V> {
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
