/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

pub mod heap;
pub mod record;
pub mod types;

use crate::constants::RelationIdT;
use crate::relation::heap::{Heap, HeapError};
use crate::relation::record::{Record, RecordId};
use crate::relation::types::{size_of, DataType};

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

/// A schema defines the structure of a single relation in the database.
/// A schema is comprised of attributes, which each define details about a single column in the
/// relation.
///
/// Example:
/// Suppose we define a relation for students at a university.
/// Attributes may include "full_name", "year_enrolled", "field_of_study", each with different
/// metadata such as the data type, or whether the field is nullable.
/// The schema is defined as the collection of each defined attribute.

#[derive(Debug)]
pub struct Schema {
    attributes: Vec<Attribute>,
    byte_len: u32,
}

impl Schema {
    /// Create a new schema with a vector of attributes, parsed from left-to-right.
    pub fn new(attributes: Vec<Attribute>) -> Self {
        let mut byte_len = 0;
        let mut attrs = attributes.iter();
        while let Some(attr) = attrs.next() {
            byte_len += size_of(attr.get_data_type());
        }

        Self {
            attributes,
            byte_len,
        }
    }

    /// Return the number of the attributes in this schema.
    pub fn attr_len(&self) -> u32 {
        self.attributes.len() as u32
    }

    /// Return this schema's attributes.
    pub fn get_attributes(&self) -> &[Attribute] {
        self.attributes.as_slice()
    }

    /// Return the number of bytes of the fixed-length values of a record defined by this schema.
    /// Variable-length values such as varchar are encoded as a fixed-length offset/length pair.
    pub fn byte_len(&self) -> u32 {
        self.byte_len
    }

    /// Return the index of the column which corresponds to the given attribute.
    /// Attributes can be queried by passing in the name as a string slice.
    pub fn get_column_index(&self, attr_name: &str) -> Option<u32> {
        for (i, attr) in self.attributes.iter().enumerate() {
            if attr.get_name() == attr_name {
                return Some(i as u32);
            }
        }
        None
    }
}

/// An attribute describes details about a single column in a record, such as its name, data
/// type, and whether it can be null.

#[derive(Debug)]
pub struct Attribute {
    name: String,
    data_type: DataType,
    primary: bool,
    serial: bool,
    nullable: bool,
}

impl Attribute {
    pub fn new(
        name: &str,
        data_type: DataType,
        primary: bool,
        serial: bool,
        nullable: bool,
    ) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            primary,
            serial,
            nullable,
        }
    }

    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }

    pub fn get_data_type(&self) -> DataType {
        self.data_type
    }

    pub fn is_primary(&self) -> bool {
        self.primary
    }

    pub fn is_serial(&self) -> bool {
        self.serial
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
}
