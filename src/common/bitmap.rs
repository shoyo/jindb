/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

/// Utility functions for interacting with bitmaps.

/// Return the n-th bit in the 32-bit bitmap.
pub fn get_nth_bit(bitmap: &u32, n: u32) -> Result<u32, BitmapErr> {
    if n >= 32 {
        return Err(BitmapErr::OutOfBounds);
    }
    Ok((*bitmap >> n) & 1u32)
}

/// Set the n-th bit in the 32-bit bitmap to 1.
pub fn set_nth_bit(bitmap: &mut u32, n: u32) -> Result<(), BitmapErr> {
    if n >= 32 {
        return Err(BitmapErr::OutOfBounds);
    }
    *bitmap |= 1u32 << n;
    Ok(())
}

/// Set the n-th bit in the 32-bit bitmap to 0.
pub fn clear_nth_bit(bitmap: &mut u32, n: u32) -> Result<(), BitmapErr> {
    if n >= 32 {
        return Err(BitmapErr::OutOfBounds);
    }
    *bitmap &= !(1u32 << n);
    Ok(())
}

/// Custom error for bitmap operations.
#[derive(Debug)]
pub enum BitmapErr {
    OutOfBounds,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_operations() {
        let mut bitmap = 12; // 0b00001100
        assert_eq!(get_nth_bit(&bitmap, 0).unwrap(), 0);
        assert_eq!(get_nth_bit(&bitmap, 3).unwrap(), 1);
        assert_eq!(get_nth_bit(&bitmap, 4).unwrap(), 0);

        set_nth_bit(&mut bitmap, 0).unwrap();
        clear_nth_bit(&mut bitmap, 3).unwrap();

        assert_eq!(bitmap, 5); // 0b00000101
    }
}
