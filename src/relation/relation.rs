/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use super::heap::Heap;
use super::schema::Schema;
use crate::common::{RecordIdT, RelationIdT};
use crate::relation::record::Record;

/// Database relation (i.e. table) represented on disk.
pub struct Relation {
    /// Unique ID for this relation
    id: RelationIdT,

    /// User-defined name for this relation
    name: String,

    /// Schema for the attributes of this relation
    schema: Schema,

    /// Collection of pages on disk which contain records
    heap: Heap,
}

impl Relation {
    /// Initialize a new in-memory representation of a relation.
    pub fn new(id: RelationIdT, name: String, schema: Schema, heap: Heap) -> Self {
        Self {
            id,
            name,
            schema,
            heap,
        }
    }

    /// Return the relation ID.
    pub fn get_id(&self) -> RelationIdT {
        self.id
    }

    /// Return the name of this relation.
    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }

    /// Return an immutable reference to this relation's schema.
    pub fn get_schema(&self) -> &Schema {
        &self.schema
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
