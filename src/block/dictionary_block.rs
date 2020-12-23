/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

/// The block with ID equal to 0 is a special block designated as the "dictionary block", which
/// stores metadata for the relations and indexes in the database.
/// Specifically, it stores key value pairs of relation/index names and their corresponding root
/// block ID. It also stores the total number of pairs in the block.
///
/// Whenever a relation or index is needed, the corresponding root block ID is looked up in this
/// dictionary block. The dictionary block is always pinned to the buffer pool while the
/// database is running.
///
/// Data format (number denotes size in bytes):
/// +---------------+-------------------+---------------------------+-----+
/// | NUM PAIRS (4) | ENTRY 1 NAME (64) | ENTRY 1 ROOT BLOCK ID (4) | ... |
/// +---------------+-------------------+---------------------------+-----+

pub struct DictionaryBlock;

impl DictionaryBlock {
    pub fn new() -> Self {
        todo!()
    }
}
