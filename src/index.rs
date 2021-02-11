/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jindb for more information about this project and its license.
 */

use crate::relation::record::{Record, RecordId};
use crate::relation::Schema;
use std::sync::Arc;

pub trait Index {
    fn get(key: &Record) -> Vec<RecordId>;

    fn set(key: &Record, rid: RecordId);

    fn delete(key: &Record, rid: RecordId);
}

pub struct IndexMeta {
    name: String,
    table_name: String,
    schema: Arc<Schema>,
}
