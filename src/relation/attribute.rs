/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

/// A single attribute in a relation. (i.e. "columns" in a table)
#[derive(Debug)]
pub struct Attribute {
    pub name: String,
    pub data_type: DataType,
    pub primary: bool,
    pub serial: bool,
    pub nullable: bool,
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
