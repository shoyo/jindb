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
    pub fn attr_len(&self) -> usize {
        self.attributes.len()
    }

    /// Return the number of bytes of the fixed-length values of a record defined by this schema.
    /// Variable-length values such as varchar are encoded as a fixed-length offset/length pair.
    pub fn byte_len(&self) -> usize {
        let mut len = 0;
        let mut attrs = self.attributes.iter();
        while let Some(attr) = attrs.next() {
            match attr.data_type {
                DataType::Boolean => len += 1,
                DataType::TinyInt => len += 1,
                DataType::SmallInt => len += 2,
                DataType::Int => len += 4,
                DataType::BigInt => len += 8,
                DataType::Decimal => len += 8,
                DataType::Varchar => len += 8,
            }
        }
        len
    }

    /// Return the index of the column which corresponds to the given attribute.
    /// Attributes can be queried by passing in the name as a string slice.
    pub fn get_column_index(&self, attr_name: &str) -> Option<usize> {
        for (i, attr) in self.attributes.iter().enumerate() {
            if &attr.name == attr_name {
                return Some(i);
            }
        }
        None
    }
}

/// A single attribute in a relation. (i.e. "columns" in a table)
#[derive(Debug)]
pub struct Attribute {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Data types for values in the database.
#[derive(Debug)]
pub enum DataType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Decimal,
    Varchar,
}
