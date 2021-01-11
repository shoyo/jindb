/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

/// Mapping between internal and built-in data types.
pub type BOOLEAN = bool;
pub type TINYINT = i8;
pub type SMALLINT = i16;
pub type INT = i32;
pub type BIGINT = i64;
pub type DECIMAL = f32;
pub type VARCHAR = String;

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

/// Internal data types for values in the database.
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

/// An enum for contained values in a Value trait.
pub enum InnerValue {
    Boolean(BOOLEAN),
    TinyInt(TINYINT),
    SmallInt(SMALLINT),
    Int(INT),
    BigInt(BIGINT),
    Decimal(DECIMAL),
    Varchar(VARCHAR),
}

/// Shared interface for custom data types.
pub trait Value {
    /// Return the contained value.
    fn inner(&self) -> InnerValue;

    /// Return the data type of the contained value.
    fn data_type(&self) -> DataType;
}

/// INTERNAL DATA TYPES

impl Value for BOOLEAN {
    fn inner(&self) -> InnerValue {
        InnerValue::Boolean(*self)
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }
}

impl Value for TINYINT {
    fn inner(&self) -> InnerValue {
        InnerValue::TinyInt(*self)
    }

    fn data_type(&self) -> DataType {
        DataType::TinyInt
    }
}

impl Value for SMALLINT {
    fn inner(&self) -> InnerValue {
        InnerValue::SmallInt(*self)
    }

    fn data_type(&self) -> DataType {
        DataType::SmallInt
    }
}

impl Value for INT {
    fn inner(&self) -> InnerValue {
        InnerValue::Int(*self)
    }

    fn data_type(&self) -> DataType {
        DataType::Int
    }
}

impl Value for BIGINT {
    fn inner(&self) -> InnerValue {
        InnerValue::BigInt(*self)
    }

    fn data_type(&self) -> DataType {
        DataType::BigInt
    }
}

impl Value for DECIMAL {
    fn inner(&self) -> InnerValue {
        InnerValue::Decimal(*self)
    }

    fn data_type(&self) -> DataType {
        DataType::Decimal
    }
}

impl Value for VARCHAR {
    fn inner(&self) -> InnerValue {
        InnerValue::Varchar(self.clone())
    }

    fn data_type(&self) -> DataType {
        DataType::Varchar
    }
}
