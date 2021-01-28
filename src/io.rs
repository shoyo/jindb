/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

/// Utility functions for reading and writing byte arrays.

/// Read a boolean at the specified offset in the byte array.
#[inline]
pub fn read_bool(array: &[u8], offset: u32) -> Result<bool, IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 1)?;

    let byte = array[offset];
    match array[offset] {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(IoError::Custom(format!(
            "Expected either 0 or 1 for underlying bool representation, found: {}",
            byte
        ))),
    }
}

/// Write a boolean at the specified offset in the byte array.
#[inline]
pub fn write_bool(array: &mut [u8], offset: u32, value: bool) -> Result<(), IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 1)?;

    match value {
        true => array[offset] = 1u8,
        false => array[offset] = 0u8,
    }

    Ok(())
}

/// Read an unsigned 32-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_u32(array: &[u8], offset: u32) -> Result<u32, IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 4)?;

    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = array[offset + i];
    }

    Ok(u32::from_le_bytes(bytes))
}

/// Write an unsigned 32-bit integer at the specified offset in the byte array. Any existing
/// value is overwritten.
#[inline]
pub fn write_u32(array: &mut [u8], offset: u32, value: u32) -> Result<(), IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 4)?;

    for i in 0..4 {
        array[offset + i] = ((value >> (i * 8)) & 0xff) as u8;
    }

    Ok(())
}

/// Read an unsigned 64-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_u64(array: &[u8], offset: u32) -> Result<u64, IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 8)?;

    let mut bytes = [0; 8];
    for i in 0..8 {
        bytes[i] = array[offset + i];
    }

    Ok(u64::from_le_bytes(bytes))
}

/// Write an unsigned 64-bit integer at the specified offset in the byte array. Any existing
/// value is overwritten.
#[inline]
pub fn write_u64(array: &mut [u8], offset: u32, value: u64) -> Result<(), IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 8)?;

    for i in 0..8 {
        array[offset + i] = ((value >> (i * 8)) & 0xff) as u8;
    }

    Ok(())
}

/// Read a signed 8-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_i8(array: &[u8], offset: u32) -> Result<i8, IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 1)?;

    Ok(i8::from_le_bytes([array[offset]]))
}

/// Write a signed 8-bit integer at the specified offset in the byte array.
#[inline]
pub fn write_i8(array: &mut [u8], offset: u32, value: i8) -> Result<(), IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 1)?;

    array[offset] = value as u8;

    Ok(())
}

/// Read a signed 16-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_i16(array: &[u8], offset: u32) -> Result<i16, IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 2)?;

    let mut bytes = [0; 2];
    bytes[0] = array[offset];
    bytes[1] = array[offset + 1];

    Ok(i16::from_le_bytes(bytes))
}

/// Write a signed 16-bit integer at the specified offset in the byte array.
#[inline]
pub fn write_i16(array: &mut [u8], offset: u32, value: i16) -> Result<(), IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 2)?;

    array[offset] = (value & 0xff) as u8;
    array[offset + 1] = ((value >> 8) & 0xff) as u8;

    Ok(())
}

/// Read a signed 32-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_i32(array: &[u8], offset: u32) -> Result<i32, IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 4)?;

    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = array[offset + i];
    }

    Ok(i32::from_le_bytes(bytes))
}

/// Write a signed 32-bit integer at the specified offset in the byte array.
#[inline]
pub fn write_i32(array: &mut [u8], offset: u32, value: i32) -> Result<(), IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 4)?;

    for i in 0..4 {
        array[offset + i] = ((value >> (i * 8)) & 0xff) as u8;
    }

    Ok(())
}

/// Read a signed 64-bit integer at the specified offset in the byte array.
#[inline]
pub fn read_i64(array: &[u8], offset: u32) -> Result<i64, IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 8)?;

    let mut bytes = [0; 8];
    for i in 0..8 {
        bytes[i] = array[offset + i];
    }

    Ok(i64::from_le_bytes(bytes))
}

/// Write a signed 64-bit integer at the specified offset in the byte array.
#[inline]
pub fn write_i64(array: &mut [u8], offset: u32, value: i64) -> Result<(), IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 8)?;

    for i in 0..8 {
        array[offset + i] = ((value >> (i * 8)) & 0xff) as u8;
    }

    Ok(())
}

/// Read a signed 32-bit float at the specified offset in the byte array.
#[inline]
pub fn read_f32(array: &[u8], offset: u32) -> Result<f32, IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 4)?;

    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = array[offset + i];
    }

    Ok(f32::from_le_bytes(bytes))
}

/// Write a signed 32-bit float at the specified offset in the byte array.
#[inline]
pub fn write_f32(array: &mut [u8], offset: u32, value: f32) -> Result<(), IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, 4)?;

    let bytes = f32::to_le_bytes(value);

    for i in 0..4 {
        array[offset + i] = bytes[i];
    }

    Ok(())
}

/// Read a variable-length string with a specified offset/length in the byte array.
#[inline]
pub fn read_str(array: &[u8], offset: u32, length: u32) -> Result<String, IoError> {
    let offset = offset as usize;
    let length = length as usize;
    check_overflow(array.len(), offset, length)?;

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
pub fn write_str(array: &mut [u8], offset: u32, string: &str) -> Result<(), IoError> {
    let offset = offset as usize;
    check_overflow(array.len(), offset, string.len())?;

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
    read_str(array, offset, 32)
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
    write_str(array, offset, string)
}

/// Return an Error if inserting data of specified offset/length into an array of a given
/// array_len would cause an overflow.
#[inline(always)]
fn check_overflow(array_len: usize, offset: usize, length: usize) -> Result<(), IoError> {
    if offset + length > array_len {
        return Err(IoError::Overflow);
    }
    Ok(())
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
    use crate::constants::PAGE_SIZE;

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
    fn test_read_write_u64() {
        let mut array = vec![0; 100];
        let offset = 72;
        let value = 980981237789123_u64;

        let result = write_u64(array.as_mut_slice(), offset, value);
        assert!(result.is_ok());

        let result = read_u64(array.as_slice(), offset);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value)
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

    #[test]
    fn test_read_write_string() {
        let mut array = vec![0; 100];
        let offset = 25;
        let value = "hello, world! foo bar baz.";

        let result = write_str(array.as_mut_slice(), offset, value);
        assert!(result.is_ok());

        let result = read_str(array.as_slice(), offset, value.len() as u32);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value.to_string());
    }

    #[test]
    fn test_read_write_bool() {
        let mut array = vec![0; 100];
        let offset = 43;
        let value = true;

        let result = write_bool(array.as_mut_slice(), offset, value);
        assert!(result.is_ok());

        let result = read_bool(array.as_slice(), offset);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value)
    }

    #[test]
    fn test_read_write_i8() {
        let mut array = vec![0; 100];
        let offset = 43;
        let value = -100;

        let result = write_i8(array.as_mut_slice(), offset, value);
        assert!(result.is_ok());

        let result = read_i8(array.as_slice(), offset);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value)
    }

    #[test]
    fn test_read_write_i16() {
        let mut array = vec![0; 100];
        let offset = 43;
        let value = -300;

        let result = write_i16(array.as_mut_slice(), offset, value);
        assert!(result.is_ok());

        let result = read_i16(array.as_slice(), offset);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value)
    }

    #[test]
    fn test_read_write_i32() {
        let mut array = vec![0; 100];
        let offset = 43;
        let value = -70_000;

        let result = write_i32(array.as_mut_slice(), offset, value);
        assert!(result.is_ok());

        let result = read_i32(array.as_slice(), offset);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value)
    }

    #[test]
    fn test_read_write_i64() {
        let mut array = vec![0; 100];
        let offset = 43;
        let value = -5_000_000_000;

        let result = write_i64(array.as_mut_slice(), offset, value);
        assert!(result.is_ok());

        let result = read_i64(array.as_slice(), offset);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value)
    }

    #[test]
    fn test_read_write_f32() {
        let mut array = vec![0; 100];
        let offset = 43;
        let value = -76_543.21;

        let result = write_f32(array.as_mut_slice(), offset, value);
        assert!(result.is_ok());

        let result = read_f32(array.as_slice(), offset);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value)
    }
}
