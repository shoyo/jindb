/// Global configuration file

pub const DB_FILENAME: &str = "db.minusql";
pub const BLOCK_SIZE: u32 = 64;

/// Constants for slotted-page header
pub const BLOCK_ID_OFFSET: u32 = 0;
pub const PREV_BLOCK_ID_OFFSET: u32 = 4;
pub const NEXT_BLOCK_ID_OFFSET: u32 = 8;
pub const FREE_POINTER_OFFSET: u32 = 12;
pub const NUM_RECORDS_OFFSET: u32 = 16;
pub const LSN_OFFSET: u32 = 20;
pub const RECORDS_OFFSET: u32 = 24;
pub const RECORD_POINTER_SIZE: u32 = 8;
