/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

/// Type aliases
pub type BlockIdT = u32;
pub type RelationIdT = u32;
pub type RecordIdT = u32;
pub type BufferFrameIdT = u32;
pub type TransactionIdT = u32;
pub type LsnT = u32;

/// Global constants
pub const DB_FILENAME: &str = "db.jin";
pub const BLOCK_SIZE: u32 = 4096;
pub const INVALID_BLOCK_ID: BlockIdT = 0;
pub const INVALID_LSN: LsnT = 0;
pub const BUFFER_SIZE: BufferFrameIdT = 1024;
