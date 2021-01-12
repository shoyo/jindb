/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

/// Utility functions for reading and writing byte arrays.

/// Read a boolean at the specified offset in the byte array.
#[inline]
pub fn read_bool(array: &[u8], offset: u32) -> Result<bool, IoError> {
    todo!()
}

/// Write a boolean at the specified offset in the byte array.
#[inline]
pub fn write_bool(array: &mut [u8], offset: u32, value: bool) -> Result<(), IoError> {
    todo!()
}

/// Read an unsigned 32-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_u32(array: &[u8], offset: u32) -> Result<u32, IoError> {
    let offset = offset as usize;
    if offset + 4 > array.len() {
        return Err(IoError::Overflow);
    }

    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = array[offset + i];
    }
    let value = u32::from_le_bytes(bytes);
    Ok(value)
}

/// Write an unsigned 32-bit integer at the specified offset in the byte array. Any existing
/// value is overwritten.
#[inline]
pub fn write_u32(array: &mut [u8], offset: u32, value: u32) -> Result<(), IoError> {
    let offset = offset as usize;
    if offset + 4 > array.len() {
        return Err(IoError::Overflow);
    }

    array[offset] = (value & 0xff) as u8;
    array[offset + 1] = ((value >> 8) & 0xff) as u8;
    array[offset + 2] = ((value >> 16) & 0xff) as u8;
    array[offset + 3] = ((value >> 24) & 0xff) as u8;
    Ok(())
}

/// Read a signed 8-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_i8(array: &[u8], offset: u32) -> Result<i8, IoError> {
    todo!()
}

/// Write a signed 8-bit integer at the specified offset in the byte array.
#[inline]
pub fn write_i8(array: &mut [u8], offset: u32, value: i8) -> Result<(), IoError> {
    todo!()
}

/// Read a signed 16-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_i16(array: &[u8], offset: u32) -> Result<i16, IoError> {
    todo!()
}

/// Write a signed 16-bit integer at the specified offset in the byte array.
#[inline]
pub fn write_i16(array: &mut [u8], offset: u32, value: i16) -> Result<(), IoError> {
    todo!()
}

/// Read a signed 32-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_i32(array: &[u8], offset: u32) -> Result<i32, IoError> {
    todo!()
}
/// Write a signed 32-bit integer at the specified offset in the byte array.
#[inline]
pub fn write_i32(array: &mut [u8], offset: u32, value: i32) -> Result<(), IoError> {
    todo!()
}

/// Read a signed 64-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_i64(array: &[u8], offset: u32) -> Result<i64, IoError> {
    todo!()
}

/// Write a signed 64-bit integer at the specified offset in the byte array.
#[inline]
pub fn write_i64(array: &mut [u8], offset: u32, value: i64) -> Result<(), IoError> {
    todo!()
}

/// Read a signed 32-bit float at the specified offset in the byte array.
#[inline]
pub fn read_f32(array: &[u8], offset: u32) -> Result<f32, IoError> {
    todo!()
}
/// Write a signed 32-bit float at the specified offset in the byte array.
#[inline]
pub fn write_f32(array: &mut [u8], offset: u32, value: f32) -> Result<(), IoError> {
    todo!()
}

/// Read a variable-length string with a specified offset/length in the byte array.
#[inline]
pub fn read_string(array: &[u8], offset: u32, length: u32) -> Result<String, IoError> {
    let offset = offset as usize;
    let length = length as usize;

    if offset + length > array.len() {
        return Err(IoError::Overflow);
    }

    // Scan array from right and find where null bytes end.
    let mut trim_idx = offset + length;
    for i in (offset..offset + length).rev() {
        if array[i] != 0 {
            trim_idx = i + 1;
            break;
        }
    }

    // Parse byte array without trailing null bytes into String.
    match String::from_utf8(Vec::from(&array[offset..trim_idx])) {
        Ok(s) => Ok(s),
        Err(_) => {
            return Err(IoError::Custom(format!(
                "String stored in byte array is not valid UTF-8"
            )))
        }
    }
}
/// Write a variable-length string with a specified offset/length in the byte array.
#[inline]
pub fn write_string(array: &mut [u8], offset: u32, string: &str) -> Result<(), IoError> {
    let offset = offset as usize;

    if offset + string.len() > array.len() {
        return Err(IoError::Overflow);
    }

    let bytes = string.as_bytes();
    for i in 0..bytes.len() {
        array[offset + i] = bytes[i];
    }
    Ok(())
}

/// Read a 32-byte string at the specified offset in the byte array. It is assumed that the
/// string is encoded as valid UTF-8.
#[inline]
pub fn read_str256(array: &[u8], offset: u32) -> Result<String, IoError> {
    read_string(array, offset, 32)
}

/// Write a 32-byte string at the specified offset in the byte array. Any existing value is
/// overwritten. If is assumed that the string is encoded as valid UTF-8.
#[inline]
pub fn write_str256(array: &mut [u8], offset: u32, string: &str) -> Result<(), IoError> {
    if string.as_bytes().len() > 32 {
        return Err(IoError::Custom(format!(
            "Length of string cannot exceed 32 bytes"
        )));
    }
    write_string(array, offset, string)
}

/// Custom IO-related errors.
#[derive(Debug)]
pub enum IoError {
    Overflow,
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::PAGE_SIZE;

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
        let array = [1; PAGE_SIZE as usize];

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
