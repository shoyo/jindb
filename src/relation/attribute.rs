/// A single attribute in a relation. (i.e. "columns" in a table)
#[derive(Debug)]
pub struct Attribute {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Data types for values in the database.
#[derive(Copy, Clone, Debug)]
pub enum DataType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Decimal,
    Varchar,
}

/// Return the size of a data type in bytes.
pub fn type_size(data_type: DataType) -> u8 {
    match data_type {
        DataType::Boolean => 1,
        DataType::TinyInt => 1,
        DataType::SmallInt => 2,
        DataType::Int => 4,
        DataType::BigInt => 8,
        DataType::Decimal => 8,
        DataType::Varchar => 8,
    }
}
