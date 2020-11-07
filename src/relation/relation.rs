use super::schema::Schema;
use crate::buffer::manager::BufferManager;
use crate::common::constants::BlockIdT;

/// Metadata for a database relation.
pub struct RelationMeta {
    name: String,
    schema: Schema,
    num_rows: u32,
}

/// Database relation (i.e. table) represented on disk.
/// A relation is comprised of one or more blocks on disk that point to each
/// other as a doubly linked list.
pub struct Relation {
    buffer_manager: BufferManager,
    head_block_id: BlockIdT,
}
