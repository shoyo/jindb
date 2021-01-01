/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use super::heap::Heap;
use super::schema::Schema;
use crate::common::{RecordIdT, RelationIdT};
use crate::relation::record::Record;
use std::sync::{Arc, Mutex};

pub type RelationGuard = Arc<Mutex<Relation>>;

/// Database relation (i.e. table) represented on disk.
pub struct Relation {
    /// Unique ID for this relation
    pub id: RelationIdT,

    /// User-defined name for this relation
    pub name: String,

    /// Schema for the attributes of this relation
    pub schema: Schema,

    /// Collection of pages on disk which contain records
    pub heap: Heap,
}

impl Relation {
    pub fn new(id: RelationIdT, name: String, schema: Schema, heap: Heap) -> Self {
        Self {
            id,
            name,
            schema,
            heap,
        }
    }

    pub fn insert_record(&mut self, record: Record) -> Result<(), ()> {
        self.heap.insert(record)
    }

    pub fn flag_delete_record(&mut self, record_id: RecordIdT) -> Result<(), ()> {
        self.heap.flag_delete(record_id)
    }

    pub fn commit_delete_record(&mut self, record_id: RecordIdT) -> Result<(), ()> {
        self.heap.commit_delete(record_id)
    }

    pub fn rollback_delete_record(&mut self, record_id: RecordIdT) -> Result<(), ()> {
        self.heap.rollback_delete(record_id)
    }
}
