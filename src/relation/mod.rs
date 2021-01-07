/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

pub mod attribute;
pub mod heap;
pub mod iterator;
pub mod record;
pub mod schema;

use crate::common::RelationIdT;
use crate::relation::heap::Heap;
use crate::relation::record::{Record, RecordId};
use crate::relation::schema::Schema;

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

    /// Insert a record into this relation.
    pub fn insert_record(&mut self, record: Record) -> Result<(), ()> {
        self.heap.insert(record)
    }

    /// Update a record in this relation.
    pub fn update_record(&mut self, record: Record) -> Result<(), ()> {
        self.heap.update(record)
    }

    /// Flag a record in this relation for deletion.
    pub fn flag_delete_record(&mut self, record_id: RecordId) -> Result<(), ()> {
        self.heap.flag_delete(record_id)
    }

    /// Commit a delete operation for a record in this relation.
    pub fn commit_delete_record(&mut self, record_id: RecordId) -> Result<(), ()> {
        self.heap.commit_delete(record_id)
    }

    /// Rollback a delete operation for a record in this relation.
    pub fn rollback_delete_record(&mut self, record_id: RecordId) -> Result<(), ()> {
        self.heap.rollback_delete(record_id)
    }
}
