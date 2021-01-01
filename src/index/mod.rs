/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::execution::transaction::Transaction;
use crate::relation::record::{Record, RecordId};
use crate::relation::schema::Schema;

pub mod btree_index;
pub mod hash_table_index;

pub trait Index<'a> {
    fn get(key: &Record, txn: &'a Transaction) -> Vec<RecordId>;

    fn set(key: &Record, rid: RecordId, txn: &'a Transaction);

    fn delete(key: &Record, rid: RecordId, txn: &'a Transaction);
}

pub struct IndexMeta<'a> {
    name: String,
    table_name: String,
    schema: &'a Schema,
}
