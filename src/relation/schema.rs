use crate::relation::attribute::{type_size, Attribute};
use std::convert::TryInto;

/// A schema which defines a collection of attributes for a relation.
#[derive(Debug)]
pub struct Schema {
    pub attributes: Vec<Attribute>,
}

impl Schema {
    /// Initialize a new schema with a vector of attributes, parsed from left-to-right.
    pub fn new(attributes: Vec<Attribute>) -> Self {
        Self { attributes }
    }

    /// Return the number of the attributes in this schema.
    pub fn attr_len(&self) -> u32 {
        self.attributes.len().try_into().unwrap()
    }

    /// Return the number of bytes of the fixed-length values of a record defined by this schema.
    /// Variable-length values such as varchar are encoded as a fixed-length offset/length pair.
    pub fn byte_len(&self) -> u32 {
        let mut len = 0;
        let mut attrs = self.attributes.iter();
        while let Some(attr) = attrs.next() {
            len += type_size(attr.data_type);
        }
        len.try_into().unwrap()
    }

    /// Return the index of the column which corresponds to the given attribute.
    /// Attributes can be queried by passing in the name as a string slice.
    pub fn get_column_index(&self, attr_name: &str) -> Option<u32> {
        for (i, attr) in self.attributes.iter().enumerate() {
            if &attr.name == attr_name {
                return Some(i.try_into().unwrap());
            }
        }
        None
    }
}
