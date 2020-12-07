/// A schema which defines a collection of attributes for a relation.
pub struct Schema {
    attributes: Vec<Attribute>,
}

impl Schema {
    /// Initialize a new schema with a vector of attributes, parsed from
    /// left-to-right.
    pub fn new(attributes: Vec<Attribute>) -> Self {
        Self { attributes }
    }
}

/// A single attribute in a relation. (i.e. "columns" in a table)
pub struct Attribute {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Data types for values in the database.
pub enum DataType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Decimal,
    Varchar,
}
