/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

pub mod header_block;
pub mod table_block;

/// A trait for defining shared behavior between blocks.
/// Blocks can store various things, such as metadata (header block), relation data (relation
/// blocks), index headers (index header blocks) and indexes (index blocks).
pub trait Block {}
