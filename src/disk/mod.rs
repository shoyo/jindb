/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use std::fs::{File, OpenOptions};

pub mod manager;

/// Utility functions for disk management.

/// Open a file in write-mode.
fn open_write_file(filename: &str) -> File {
    OpenOptions::new()
        .create(true)
        .write(true)
        .open(filename)
        .unwrap()
}
