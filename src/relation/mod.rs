/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

pub mod attribute;
pub mod heap;
pub mod iterator;
pub mod record;
pub mod schema;
pub mod types;

use crate::common::RelationIdT;
use crate::relation::heap::{Heap, HeapError};
use crate::relation::record::{Record, RecordId};
use crate::relation::schema::Schema;

use std::sync::Arc;

/// Database relation (i.e. table) represented on disk.
pub struct Relation {
    /// Unique ID for this relation
    id: RelationIdT,

    /// User-defined name for this relation
    name: String,

    /// Schema for the attributes of this relation
    schema: Arc<Schema>,

    /// Collection of pages on disk which contain records
    heap: Arc<Heap>,
}

impl Relation {
    /// Initialize a new in-memory representation of a relation.
    pub fn new(id: RelationIdT, name: String, schema: Arc<Schema>, heap: Arc<Heap>) -> Self {
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
    pub fn get_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Read and return a record from this relation.
    pub fn read(&self, rid: RecordId) -> Result<Record, HeapError> {
        self.heap.read(rid)
    }

    /// Insert a record into this relation. Return the record ID of the inserted record.
    pub fn insert(&self, record: Record) -> Result<RecordId, HeapError> {
        self.heap.insert(record)
    }

    /// Update a record in this relation.
    pub fn update(&self, record: Record, rid: RecordId) -> Result<(), HeapError> {
        self.heap.update(record, rid)
    }

    /// Flag a record in this relation for deletion.
    pub fn flag_delete(&self, rid: RecordId) -> Result<(), ()> {
        self.heap.flag_delete(rid)
    }

    /// Commit a delete operation for a record in this relation.
    pub fn commit_delete(&self, rid: RecordId) -> Result<(), ()> {
        self.heap.commit_delete(rid)
    }

    /// Rollback a delete operation for a record in this relation.
    pub fn rollback_delete(&mut self, rid: RecordId) -> Result<(), ()> {
        self.heap.rollback_delete(rid)
    }
}
