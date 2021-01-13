/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::bitmap::{get_nth_bit, set_nth_bit};
use crate::common::io::{
    write_bool, write_f32, write_i16, write_i32, write_i64, write_i8, write_str, write_u32,
};
use crate::common::{PageIdT, RecordSlotIdT};
use crate::relation::schema::Schema;
use crate::relation::types::{size_of, DataType, InnerValue, Value};
use std::collections::VecDeque;
use std::sync::Arc;

/// Constants for record offsets.
const FIXED_VALUES_OFFSET: u32 = 4;

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
    pub fn new(
        values: Vec<Option<Box<dyn Value>>>,
        schema: Arc<Schema>,
    ) -> Result<Self, RecordErr> {
        // Assert that values and schema are the same length.
        if values.len() as u32 != schema.attr_len() {
            return Err(RecordErr::ValSchemaMismatch);
        }

        // Empty byte vector and bitmap to be owned by new record.
        let mut bytes: Vec<u8> = Vec::new();
        let mut bitmap: u32 = 0;

        // Byte array address to begin writing values.
        let mut addr = FIXED_VALUES_OFFSET;

        // Queue to keep track of offsets for varchars.
        let mut varchars: Vec<(u32, String)> = Vec::new();

        // 1) Write the fixed-length values into the byte vector.
        for (i, (val, attr)) in values.iter().zip(schema.attributes.iter()).enumerate() {
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
                                panic!("Encountered invalid inner value type");
                            }
                        }
                        DataType::TinyInt => {
                            if let InnerValue::TinyInt(inner) = value.get_inner() {
                                write_i8(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 1;
                            } else {
                                panic!("Encountered invalid inner value type");
                            }
                        }
                        DataType::SmallInt => {
                            if let InnerValue::SmallInt(inner) = value.get_inner() {
                                write_i16(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 2;
                            } else {
                                panic!("Encountered invalid inner value type");
                            }
                        }
                        DataType::Int => {
                            if let InnerValue::Int(inner) = value.get_inner() {
                                write_i32(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 4;
                            } else {
                                panic!("Encountered invalid inner value type");
                            }
                        }
                        DataType::BigInt => {
                            if let InnerValue::BigInt(inner) = value.get_inner() {
                                write_i64(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 8;
                            } else {
                                panic!("Encountered invalid inner value type");
                            }
                        }
                        DataType::Decimal => {
                            if let InnerValue::Decimal(inner) = value.get_inner() {
                                write_f32(bytes.as_mut_slice(), addr, inner).unwrap();
                                addr += 4;
                            } else {
                                panic!("Encountered invalid inner value type");
                            }
                        }
                        DataType::Varchar => {
                            if let InnerValue::Varchar(inner) = value.get_inner() {
                                // Allocate space for offset/length and write the length as a fixed-length
                                // value for now.
                                // Offset and actual string data will be handled after all fixed-lengths are
                                // written.
                                varchars.push((addr, inner.clone()));
                                write_u32(bytes.as_mut_slice(), addr + 4, inner.len() as u32);
                                addr += 8; // Increment by length of 2 unsigned 32-bit integers.
                            } else {
                                panic!("Encountered invalid inner value");
                            }
                        }
                    }
                }
                None => {
                    if !attr.is_nullable() {
                        return Err(RecordErr::NotNullable);
                    }
                    set_nth_bit(&mut bitmap, i as u32);
                    addr += size_of(attr.get_data_type());
                }
            }
        }

        // 2) Write the variable-length values and offsets into the byte vector.
        for (offset, varchar) in varchars.iter() {
            write_str(bytes.as_mut_slice(), addr, varchar).unwrap();
            write_u32(bytes.as_mut_slice(), *offset, addr);
            addr += varchar.len() as u32;
        }

        Ok(Self {
            id: None,
            bytes,
            schema,
            bitmap,
        })
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
        get_nth_bit(&self.bitmap, idx).unwrap() == 1
    }

    /// Index the schema and set the corresponding value contained in the Record to null. panic
    /// if the specified index is out-of-bounds.
    pub fn set_null(&mut self, idx: u32) {
        if idx >= self.schema.attr_len() {
            panic!("Specified index is out-of-bounds");
        }
        set_nth_bit(&mut self.bitmap, idx).unwrap();
    }
}

/// A database record descriptor, comprised of the page ID and slot index that
/// the record is located at.
#[derive(Debug)]
pub struct RecordId {
    pub page_id: PageIdT,
    pub slot_index: RecordSlotIdT,
}

/// Custom error to be used by Record.
pub enum RecordErr {
    ValSchemaMismatch,
    NotNullable,
}

#[cfg(test)]
mod tests {
    use crate::relation::attribute::Attribute;
    use crate::relation::schema::Schema;
    use crate::relation::types::DataType;

    #[test]
    fn test_create_record() {
        let schema = Schema::new(vec![
            Attribute::new("foo", DataType::Int, false, false, false);
        ]);
    }
}
