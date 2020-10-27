/// Global configuration file

const DB_FILENAME: &str = "db.minusql";
const BLOCK_SIZE: u32 = 64;

/// Constants for slotted-page header
const BLOCK_ID_OFFSET: u32 = 0;
const FREE_POINTER_OFFSET: u32 = 4;
const NUM_RECORDS_OFFSET: u32 = 8;
const RECORDS_OFFSET: u32 = 12;
const RECORD_POINTER_SIZE: u32 = 8;
