/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{LsnT, PageIdT, PAGE_SIZE};

pub mod dictionary_page;
pub mod relation_page;

/// A trait for pages stored in the database. A page, regardless of its variant, is
/// common::PAGE_SIZE bytes in length.
/// Pages can store various things, such as metadata (dictionary page), relation data (relation
/// pages), index headers (index header pages) and indexes (index pages).
pub trait Page {
    fn get_id(&self) -> PageIdT;

    fn get_pin_count(&self) -> u32;

    fn incr_pin_count(&mut self);

    fn decr_pin_count(&mut self);

    fn is_dirty(&self) -> bool;

    fn set_dirty_flag(&mut self, flag: bool);

    fn get_lsn(&self) -> LsnT;

    fn set_lsn(&mut self, lsn: LsnT);
}

/// Page variants
pub enum PageVariant {
    Dictionary,
    Relation,
}

/// Utility functions for reading and writing byte arrays.

/// Read an unsigned 32-bit integer at the specified location in the byte array.
#[inline]
pub fn read_u32(array: &[u8; PAGE_SIZE as usize], addr: u32) -> Result<u32, String> {
    if addr + 4 > PAGE_SIZE {
        return Err(overflow_error());
    }
    let addr = addr as usize;
    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = array[addr + i];
    }
    let value = u32::from_le_bytes(bytes);
    Ok(value)
}

/// Write an unsigned 32-bit integer at the specified location in the byte array. Any existing
/// value is overwritten.
#[inline]
pub fn write_u32(
    array: &mut [u8; PAGE_SIZE as usize],
    addr: u32,
    value: u32,
) -> Result<(), String> {
    if addr + 4 > PAGE_SIZE {
        return Err(overflow_error());
    }
    let addr = addr as usize;
    array[addr] = (value & 0xff) as u8;
    array[addr + 1] = ((value >> 8) & 0xff) as u8;
    array[addr + 2] = ((value >> 16) & 0xff) as u8;
    array[addr + 3] = ((value >> 24) & 0xff) as u8;
    Ok(())
}

/// Read a 32-byte string at the specified location in the byte array. It is assumed that the
/// string is encoded as valid UTF-8.
#[inline]
pub fn read_str256(array: &[u8; PAGE_SIZE as usize], addr: u32) -> Result<String, String> {
    if addr + 32 > PAGE_SIZE {
        return Err(overflow_error());
    }
    let addr = addr as usize;

    // Scan array from right and find where null bytes end.
    let mut trim_idx = addr + 32;
    for i in (addr..addr + 32).rev() {
        if array[i] != 0 {
            trim_idx = i + 1;
            break;
        }
    }

    // Parse byte array without trailing null bytes into String.
    match String::from_utf8(Vec::from(&array[addr..trim_idx])) {
        Ok(s) => Ok(s),
        Err(_) => return Err(format!("String stored in byte array is not valid UTF-8")),
    }
}

/// Write a 32-byte string at the specified location in the byte array. Any existing value is
/// overwritten. If is assumed that the string is encoded as valid UTF-8.
#[inline]
pub fn write_str256(
    array: &mut [u8; PAGE_SIZE as usize],
    addr: u32,
    string: &str,
) -> Result<(), String> {
    if addr + 32 > PAGE_SIZE {
        return Err(overflow_error());
    }
    let addr = addr as usize;
    let bytes = string.as_bytes();
    if bytes.len() > 32 {
        return Err(format!("Length of string cannot exceed 32 bytes"));
    }
    for i in 0..bytes.len() {
        array[addr + i] = bytes[i];
    }
    Ok(())
}

/// Return an overflow error message.
#[inline(always)]
fn overflow_error() -> String {
    format!("Cannot access value from byte array address (overflow)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_u32() {
        let mut array = [0; PAGE_SIZE as usize];

        // Manually serialize expected value into byte array.
        let expected: u32 = 31415926;
        let offset = 21;
        let bytes = expected.to_le_bytes();
        for i in 0..bytes.len() {
            array[offset + i] = bytes[i];
        }

        // Assert that read value is correct.
        let result = read_u32(&array, offset as u32);
        assert!(result.is_ok());

        let actual = result.unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_read_u32_overflow() {
        let mut array = [1; PAGE_SIZE as usize];

        // Assert that read is successful with no overflow.
        let result = read_u32(&array, PAGE_SIZE - 4);
        assert!(result.is_ok());

        // Assert that read fails with an overflow.
        let result = read_u32(&array, PAGE_SIZE - 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_u32() {
        let mut array = [0; PAGE_SIZE as usize];

        // Serialize value into byte array with function.
        let value: u32 = 31415926;
        let offset = 31;
        let result = write_u32(&mut array, offset as u32, value);
        assert!(result.is_ok());

        // Assert that serialized bytes are correct.
        let bytes = value.to_le_bytes();
        for i in 0..bytes.len() {
            let expected = bytes[i];
            let actual = array[offset + i];
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_write_u32_overflow() {
        let mut array = [0; PAGE_SIZE as usize];
        let value: u32 = 31415926;

        // Assert that write is successful with no overflow.
        let result = write_u32(&mut array, PAGE_SIZE - 4, value);
        assert!(result.is_ok());

        // Assert that write fails with an overflow.
        let result = write_u32(&mut array, PAGE_SIZE - 3, value);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_str256() {
        let mut array = [0; PAGE_SIZE as usize];

        // Serialize expected string into byte array.
        let expected = "Hello, World!".to_string();
        let offset = 1262;
        let bytes = expected.as_bytes();
        for i in 0..bytes.len() {
            array[offset + i] = bytes[i];
        }

        // Assert that read string is correct.
        let result = read_str256(&array, offset as u32);
        assert!(result.is_ok());

        let actual = result.unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_write_str256() {
        let mut array = [0; PAGE_SIZE as usize];

        // Serialize value into byte array with function.
        let value = "Hello, World!".to_string();
        let offset = 1262;
        let result = write_str256(&mut array, offset as u32, &value);
        assert!(result.is_ok());

        // Assert that serialized bytes are correct.
        let bytes = value.as_bytes();
        for i in 0..bytes.len() {
            let expected = bytes[i];
            let actual = array[offset + i];
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_write_str256_too_long() {
        let mut array = [0; PAGE_SIZE as usize];
        let offset = 712;
        let long = "abcdefghijklmnopqrstuvwxyz";
        let too_long = "abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz";

        let result = write_str256(&mut array, offset as u32, long);
        assert!(result.is_ok());

        let result = write_str256(&mut array, offset as u32, too_long);
        assert!(result.is_err());
    }
}
