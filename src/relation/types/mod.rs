/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

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
pub fn size_of(data_type: DataType) -> u8 {
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

/// Mapping between custom and built-in data types.
pub type BOOLEAN = bool;
