/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::BLOCK_SIZE;

pub mod dictionary_block;
pub mod relation_block;

/// Utility functions for reading and writing byte arrays.

/// Read an unsigned 32-bit integer at the specified location in the
/// byte array.
pub fn read_u32(array: &[u8; BLOCK_SIZE as usize], addr: u32) -> Result<u32, String> {
    if addr + 4 > BLOCK_SIZE {
        return Err(format!(
            "Cannot read value from byte array address (overflow)"
        ));
    }
    let addr = addr as usize;
    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = array[addr + i];
    }
    let value = u32::from_le_bytes(bytes);
    Ok(value)
}

/// Write an unsigned 32-bit integer at the specified location in the
/// byte array. Any existing value is overwritten.
pub fn write_u32(
    array: &mut [u8; BLOCK_SIZE as usize],
    addr: u32,
    value: u32,
) -> Result<(), String> {
    if addr + 4 > BLOCK_SIZE {
        return Err(format!(
            "Cannot write value to byte array address (overflow)"
        ));
    }
    let addr = addr as usize;
    array[addr] = (value & 0xff) as u8;
    array[addr + 1] = ((value >> 8) & 0xff) as u8;
    array[addr + 2] = ((value >> 16) & 0xff) as u8;
    array[addr + 3] = ((value >> 24) & 0xff) as u8;
    Ok(())
}

/// An enum for blocks stored in the database. A block, regardless of its contents, is
/// common::BLOCK_SIZE bytes in length.
/// Blocks can store various things, such as metadata (dictionary block), relation data (relation
/// blocks), index headers (index header blocks) and indexes (index blocks).
pub enum Block {
    Dictionary(dictionary_block::DictionaryBlock),
    Relation(relation_block::RelationBlock),
}
