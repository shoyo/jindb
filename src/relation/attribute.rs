/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::relation::types::DataType;

/// An attribute describes details about a single column in a record, such as its name, data
/// type, and whether it can be null.

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
