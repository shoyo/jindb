/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use std::fmt::Formatter;

/// Mapping between internal and built-in data types.
pub type BOOLEAN = bool;
pub type TINYINT = i8;
pub type SMALLINT = i16;
pub type INT = i32;
pub type BIGINT = i64;
pub type DECIMAL = f32;
pub type VARCHAR = String;

/// Return the size of a data type in bytes.
pub fn size_of(data_type: DataType) -> u32 {
    match data_type {
        DataType::Boolean => 1,
        DataType::TinyInt => 1,
        DataType::SmallInt => 2,
        DataType::Int => 4,
        DataType::BigInt => 8,
        DataType::Decimal => 4,
        DataType::Varchar => 8,
    }
}

/// Internal data types for values in the database.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DataType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Decimal,
    Varchar,
}

/// An enum for contained values in a Value trait.
#[derive(Debug, PartialEq)]
pub enum InnerValue {
    Boolean(BOOLEAN),
    TinyInt(TINYINT),
    SmallInt(SMALLINT),
    Int(INT),
    BigInt(BIGINT),
    Decimal(DECIMAL),
    Varchar(VARCHAR),
}

impl std::fmt::Display for InnerValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InnerValue::Boolean(val) => write!(f, "{}", val),
            InnerValue::TinyInt(val) => write!(f, "{}", val),
            InnerValue::SmallInt(val) => write!(f, "{}", val),
            InnerValue::Int(val) => write!(f, "{}", val),
            InnerValue::BigInt(val) => write!(f, "{}", val),
            InnerValue::Decimal(val) => write!(f, "{}", val),
            InnerValue::Varchar(val) => write!(f, "{}", val),
        }
    }
}

/// Shared interface for custom data types.
pub trait Value {
    /// Return the contained value.
    fn get_inner(&self) -> InnerValue;

    /// Return the data type of the contained value.
    fn get_data_type(&self) -> DataType;
}

impl core::fmt::Debug for dyn Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.get_inner())
    }
}

/// INTERNAL DATA TYPES

impl Value for BOOLEAN {
    fn get_inner(&self) -> InnerValue {
        InnerValue::Boolean(*self)
    }

    fn get_data_type(&self) -> DataType {
        DataType::Boolean
    }
}

impl Value for TINYINT {
    fn get_inner(&self) -> InnerValue {
        InnerValue::TinyInt(*self)
    }

    fn get_data_type(&self) -> DataType {
        DataType::TinyInt
    }
}

impl Value for SMALLINT {
    fn get_inner(&self) -> InnerValue {
        InnerValue::SmallInt(*self)
    }

    fn get_data_type(&self) -> DataType {
        DataType::SmallInt
    }
}

impl Value for INT {
    fn get_inner(&self) -> InnerValue {
        InnerValue::Int(*self)
    }

    fn get_data_type(&self) -> DataType {
        DataType::Int
    }
}

impl Value for BIGINT {
    fn get_inner(&self) -> InnerValue {
        InnerValue::BigInt(*self)
    }

    fn get_data_type(&self) -> DataType {
        DataType::BigInt
    }
}

impl Value for DECIMAL {
    fn get_inner(&self) -> InnerValue {
        InnerValue::Decimal(*self)
    }

    fn get_data_type(&self) -> DataType {
        DataType::Decimal
    }
}

impl Value for VARCHAR {
    fn get_inner(&self) -> InnerValue {
        InnerValue::Varchar(self.clone())
    }

    fn get_data_type(&self) -> DataType {
        DataType::Varchar
    }
}
