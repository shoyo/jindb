/// Type aliases
pub type BlockIdT = u32;
pub type RelationIdT = u32;
pub type RecordIdT = u32;
pub type BufferFrameIdT = u32;

/// Global constants
pub const DB_FILENAME: &str = "db.jin";
pub const BLOCK_SIZE: u32 = 4096;
pub const INVALID_BLOCK_ID: BlockIdT = 0;
pub const BUFFER_SIZE: BufferFrameIdT = 1024;
