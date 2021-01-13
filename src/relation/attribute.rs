/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::relation::types::DataType;

/// An attribute describes details about a single column in a record, such as its name, data
/// type, and whether it can be null.

#[derive(Debug)]
pub struct Attribute {
    name: String,
    data_type: DataType,
    primary: bool,
    serial: bool,
    nullable: bool,
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

    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }

    pub fn get_data_type(&self) -> DataType {
        self.data_type
    }
}
