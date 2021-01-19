/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::relation::attribute::Attribute;
use crate::relation::types::size_of;

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
