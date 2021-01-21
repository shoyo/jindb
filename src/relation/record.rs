/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::bitmap::{get_nth_bit, set_nth_bit};
use crate::common::io::{
    read_bool, read_f32, read_i16, read_i32, read_i64, read_i8, read_str, read_u32, write_bool,
    write_f32, write_i16, write_i32, write_i64, write_i8, write_str, write_u32,
};
use crate::common::{PageIdT, RecordSlotIdT};
use crate::relation::schema::Schema;
use crate::relation::types::{size_of, DataType, InnerValue, Value};
use std::sync::Arc;

/// Constants for record offsets.
const NULL_BITMAP_LENGTH: u32 = 4;
const FIXED_VALUES_OFFSET: u32 = NULL_BITMAP_LENGTH;

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

#[derive(Clone, Debug)]
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
    pub fn new(
        values: Vec<Option<Box<dyn Value>>>,
        schema: Arc<Schema>,
    ) -> Result<Self, RecordErr> {
        // Assert that values and schema are the same length.
        if values.len() as u32 != schema.attr_len() {
            return Err(RecordErr::ValSchemaMismatch);
        }

        // Initialize empty byte vector and bitmap to be owned by new record.
        let mut bytes: Vec<u8> = vec![0; (NULL_BITMAP_LENGTH + schema.byte_len()) as usize];
        let mut bitmap: u32 = 0;

        // Byte array address to begin writing values.
        let mut addr = FIXED_VALUES_OFFSET;

        // Keep track of metadata to write to variable-length section.
        let mut varchars: Vec<(u32, String)> = Vec::new();
        let mut var_len = 0;

        // 1) Write the fixed-length values into the byte vector.
        for (i, (val, attr)) in values
            .iter()
            .zip(schema.get_attributes().iter())
            .enumerate()
        {
            match val.as_ref() {
                Some(value) => {
                    if value.get_data_type() != attr.get_data_type() {
                        return Err(RecordErr::ValSchemaMismatch);
                    }
                    match value.get_data_type() {
                        DataType::Boolean => {
                            if let InnerValue::Boolean(inner) = value.get_inner() {
                                write_bool(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 1;
                            } else {
                                unreachable!();
                            }
                        }
                        DataType::TinyInt => {
                            if let InnerValue::TinyInt(inner) = value.get_inner() {
                                write_i8(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 1;
                            } else {
                                unreachable!();
                            }
                        }
                        DataType::SmallInt => {
                            if let InnerValue::SmallInt(inner) = value.get_inner() {
                                write_i16(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 2;
                            } else {
                                unreachable!();
                            }
                        }
                        DataType::Int => {
                            if let InnerValue::Int(inner) = value.get_inner() {
                                write_i32(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 4;
                            } else {
                                unreachable!();
                            }
                        }
                        DataType::BigInt => {
                            if let InnerValue::BigInt(inner) = value.get_inner() {
                                write_i64(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 8;
                            } else {
                                unreachable!()
                            }
                        }
                        DataType::Decimal => {
                            if let InnerValue::Decimal(inner) = value.get_inner() {
                                write_f32(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 4;
                            } else {
                                unreachable!()
                            }
                        }
                        DataType::Varchar => {
                            if let InnerValue::Varchar(inner) = value.get_inner() {
                                // Allocate space for offset/length and write the length as a fixed-length
                                // value for now.
                                // Offset and actual string data will be handled after all fixed-lengths are
                                // written.
                                varchars.push((addr, inner.clone()));
                                write_u32(bytes.as_mut_slice(), addr + 4, inner.len() as u32)
                                    .unwrap();
                                addr += 8; // Increment by length of 2 unsigned 32-bit integers.
                                var_len += inner.len(); // Increase space needed for variable-length section.
                            } else {
                                unreachable!()
                            }
                        }
                    }
                }
                None => {
                    if !attr.is_nullable() {
                        return Err(RecordErr::NotNullable);
                    }
                    set_nth_bit(&mut bitmap, i as u32).unwrap();
                    addr += size_of(attr.get_data_type());
                }
            }
        }

        // 2) Write the variable-length values and offsets into the byte vector.
        bytes.extend(vec![0; var_len].iter()); // Make space for variable-length values.

        for (offset, varchar) in varchars.iter() {
            write_str(bytes.as_mut_slice(), addr, varchar).unwrap();
            write_u32(bytes.as_mut_slice(), *offset, addr).unwrap();
            addr += varchar.len() as u32;
        }

        Ok(Self {
            id: None,
            bytes,
            schema,
            bitmap,
        })
    }

    /// Return the raw byte array for this record.
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Return an immutable reference to the record ID.
    pub fn get_id(&self) -> Option<RecordId> {
        self.id
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
    pub fn get_value(&self, idx: u32) -> Result<Option<Box<dyn Value>>, RecordErr> {
        if idx >= self.schema.attr_len() {
            return Err(RecordErr::IndexOutOfBounds);
        }

        if self.is_null(idx).unwrap() {
            return Ok(None);
        }

        let mut addr = FIXED_VALUES_OFFSET;
        for (i, attr) in self.schema.get_attributes().iter().enumerate() {
            if i == idx as usize {
                let value: Box<dyn Value> = match attr.get_data_type() {
                    DataType::Boolean => Box::new(read_bool(self.bytes.as_slice(), addr).unwrap()),
                    DataType::TinyInt => Box::new(read_i8(self.bytes.as_slice(), addr).unwrap()),
                    DataType::SmallInt => Box::new(read_i16(self.bytes.as_slice(), addr).unwrap()),
                    DataType::Int => Box::new(read_i32(self.bytes.as_slice(), addr).unwrap()),
                    DataType::BigInt => Box::new(read_i64(self.bytes.as_slice(), addr).unwrap()),
                    DataType::Decimal => Box::new(read_f32(self.bytes.as_slice(), addr).unwrap()),
                    DataType::Varchar => Box::new({
                        let offset = read_u32(self.bytes.as_slice(), addr).unwrap();
                        let length = read_u32(self.bytes.as_slice(), addr + 4).unwrap();
                        read_str(self.bytes.as_slice(), offset, length).unwrap()
                    }),
                };
                return Ok(Some(value));
            }
            match attr.get_data_type() {
                DataType::Boolean => addr += 1,
                DataType::TinyInt => addr += 1,
                DataType::SmallInt => addr += 2,
                DataType::Int => addr += 4,
                DataType::BigInt => addr += 8,
                DataType::Decimal => addr += 4,
                DataType::Varchar => addr += 8,
            }
        }
        unreachable!()
    }

    /// Return the size of this record in bytes.
    pub fn len(&self) -> u32 {
        self.bytes.len() as u32
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
    pub fn is_null(&self, idx: u32) -> Result<bool, RecordErr> {
        if idx >= self.schema.attr_len() {
            return Err(RecordErr::IndexOutOfBounds);
        }

        let is_null = get_nth_bit(&self.bitmap, idx).unwrap() == 1;

        Ok(is_null)
    }

    /// Index the schema and set the corresponding value contained in the Record to null. panic
    /// if the specified index is out-of-bounds.
    pub fn set_null(&mut self, idx: u32) -> Result<(), RecordErr> {
        if idx >= self.schema.attr_len() {
            return Err(RecordErr::IndexOutOfBounds);
        }

        set_nth_bit(&mut self.bitmap, idx).unwrap();

        Ok(())
    }
}

/// A database record descriptor, comprised of the page ID and slot index that
/// the record is located at.
#[derive(Clone, Copy, Debug)]
pub struct RecordId {
    pub page_id: PageIdT,
    pub slot_index: RecordSlotIdT,
}

/// Custom error to be used by Record.
#[derive(Debug, Eq, PartialEq)]
pub enum RecordErr {
    ValSchemaMismatch,
    NotNullable,
    IndexOutOfBounds,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::attribute::Attribute;
    use crate::relation::schema::Schema;
    use crate::relation::types::DataType;

    #[test]
    fn test_create_record() {
        // Declare a relation schema.
        let schema = Arc::new(Schema::new(vec![
            Attribute::new("foo", DataType::Boolean, false, false, false),
            Attribute::new("bar", DataType::TinyInt, false, false, false),
            Attribute::new("baz", DataType::SmallInt, false, false, true),
            Attribute::new("foobar", DataType::Int, false, false, false),
            Attribute::new("barbaz", DataType::BigInt, false, false, true),
            Attribute::new("bazfoo", DataType::Decimal, false, false, true),
            Attribute::new("foobarbaz", DataType::Varchar, false, false, true),
        ]));

        // Create a series of values that are a valid instance of the schema.
        let valid_values: Vec<Option<Box<dyn Value>>> = vec![
            Some(Box::new(true)),
            Some(Box::new(12_i8)),
            None,
            Some(Box::new(7_654_321_i32)),
            Some(Box::new(-9_876_543_210_i64)),
            None,
            Some(Box::new("Hello, World!".to_string())),
        ];
        // Create a series of values that are NOT a valid instance of the schema.
        let invalid_values: Vec<Option<Box<dyn Value>>> = vec![
            Some(Box::new(true)),
            Some(Box::new(true)),
            Some(Box::new(true)),
            Some(Box::new(true)),
            Some(Box::new(true)),
            Some(Box::new(true)),
            Some(Box::new(true)),
        ];

        // Check that a record can NOT be created with invalid values.
        let result = Record::new(invalid_values, schema.clone());
        assert_eq!(result.unwrap_err(), RecordErr::ValSchemaMismatch);

        // Check that a record can be created with valid values.
        let mut record = Record::new(valid_values, schema.clone()).unwrap();

        // Check that each value contains the expected value.
        let value = record.get_value(0).unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap().get_inner(), InnerValue::Boolean(true));

        let value = record.get_value(2).unwrap();
        assert!(value.is_none());

        let value = record.get_value(6).unwrap();
        assert_eq!(
            value.unwrap().get_inner(),
            InnerValue::Varchar("Hello, World!".to_string())
        );

        let value = record.get_value(7);
        assert!(value.is_err());

        // Check that allocation behaves as expected.
        assert!(record.get_id().is_none());
        assert!(!record.is_allocated());
        record.allocate(0, 0);
        assert!(record.is_allocated());
    }
}
