/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{PageIdT, RecordSlotIdT};

use crate::relation::schema::Schema;
use crate::relation::types::{DataType, Value};
use std::sync::Arc;

/// A database record with variable-length attributes.
///
/// The initial section of the record contains a null bitmap which represents which attributes
/// are null and should be ignored.
///
/// The next section of a record contains fixed-length values. Data types such as numerics,
/// booleans, and dates are encoded as is, while variable-length data types such as varchar are
/// encoded as a offset/length pair.
///
/// The actual variable-length data is stored consecutively after the initial fixed-length
/// section and null bitmap.
///
/// Data format:
/// +-------------+---------------------+------------------------+
/// | NULL BITMAP | FIXED-LENGTH VALUES | VARIABLE-LENGTH VALUES |
/// +-------------+---------------------+------------------------+
///
/// Metadata regarding a record is written to the system catalog, which is located in a separate
/// database page. While a record exists in-memory, it maintains a reference to the schema which
/// defines its structure.

pub struct Record {
    /// Unique descriptor for this record. None if record is unallocated.
    id: Option<RecordId>,

    /// Raw byte array for this record.
    bytes: Vec<u8>,

    /// Schema that defines the structure of this record.
    schema: Arc<Schema>,

    /// A null bitmap that defines which values in the record are null.
    bitmap: u32,
}

impl Record {
    /// Create a new record.
    ///
    /// A newly created record is initially unallocated, and thus does not have a record ID. A
    /// record can be allocated by calling .allocate() with the corresponding page ID and slot
    /// index.
    pub fn new(values: Vec<Box<dyn Value>>, schema: Arc<Schema>) -> Self {
        // Initialize an empty byte vector.
        let mut bytes = Vec::new();

        // For each value, write the value to the byte vector.
        for value in values {
            match value.data_type() {
                DataType::Boolean => {}
                DataType::TinyInt => {}
                DataType::SmallInt => {}
                DataType::Int => {}
                DataType::BigInt => {}
                DataType::Decimal => {}
                DataType::Varchar => {}
            }
        }

        Self {
            id: None,
            bytes,
            schema,
            bitmap: 0,
        }
    }

    /// Return an immutable reference to the record ID.
    pub fn get_id(&self) -> Option<&RecordId> {
        self.id.as_ref()
    }

    /// Allocate a slot on disk for this record.
    pub fn allocate(&mut self, page_id: PageIdT, slot_index: RecordSlotIdT) {
        self.id = Some(RecordId {
            page_id,
            slot_index,
        });
    }

    /// Return whether this record has been allocated.
    pub fn is_allocated(&self) -> bool {
        self.id.is_some()
    }

    /// Index the schema and return the corresponding value contained in the Record. Return None
    /// if the value is null. Panic if the specified index is out-of-bounds.
    ///
    /// Example:
    ///
    /// A record has a schema with attributes: "Foo", "Bar", and "Baz" (in this order).
    ///
    /// idx = 0 returns the value for "Foo".
    /// idx = 1 returns the value for "Bar".
    /// idx = 2 returns the value for "Baz".
    /// idx > 2 would panic.
    pub fn get_value(&self, idx: u32) -> Option<Box<dyn Value>> {
        if idx >= self.schema.attr_len() {
            panic!("Specified index is out-of-bounds");
        }
        todo!()
    }

    /// Index the schema and return whether the corresponding value contained in the Record is
    /// null. Panic if the specified index is out-of-bounds.
    ///
    /// Example:
    ///
    /// A record has a schema with attributes: "Foo", "Bar", and "Baz" (in this order).
    ///
    /// idx = 0 returns whether the value for "Foo" is null.
    /// idx = 1 returns whether the value for "Bar" is null.
    /// idx = 2 returns whether the value for "Baz" is null.
    /// idx > 2 would panic.
    pub fn is_null(&self, idx: u32) -> bool {
        if idx >= self.schema.attr_len() {
            panic!("Specified index is out-of-bounds");
        }

        // Check whether the specified bit is set to 1.
        (self.bitmap >> idx) & 1 == 1
    }

    /// Index the schema and set the corresponding value contained in the Record to null. panic
    /// if the specified index is out-of-bounds.
    pub fn set_null(&mut self, idx: u32) {
        if idx >= self.schema.attr_len() {
            panic!("Specified index is out-of-bounds");
        }

        // Set specified bit to 1.
        self.bitmap = (1 << idx) | self.bitmap;
    }
}

/// A database record descriptor, comprised of the page ID and slot index that
/// the record is located at.
#[derive(Debug)]
pub struct RecordId {
    pub page_id: PageIdT,
    pub slot_index: RecordSlotIdT,
}
