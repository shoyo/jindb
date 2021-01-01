/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

/// Note: The type aliases and global constants below are primarily to improve readability
/// throughout the codebase. The values should not be configured/modified unless explicitly
/// annotated with "safe to modify".

/// Type aliases
pub type PageIdT = u32;
pub type RelationIdT = u32;
pub type RecordIdT = u32;
pub type BufferFrameIdT = u32;
pub type TransactionIdT = u32;
pub type LsnT = u32;

/// Global constants
pub const DB_FILENAME: &str = "db.jin"; // safe to modify
pub const PAGE_SIZE: u32 = 4096; // safe to modify
pub const BUFFER_SIZE: BufferFrameIdT = 1024; // safe to modify
pub const DICTIONARY_PAGE_ID: PageIdT = 0;
pub const INVALID_LSN: LsnT = 0;
