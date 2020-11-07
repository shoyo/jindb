use super::heap::Heap;
use super::schema::Schema;
use crate::common::constants::RelationIdT;

/// Database relation (i.e. table) represented on disk.
pub struct Relation {
    /// Unique ID for this relation
    id: RelationIdT,

    /// User-defined name for this relation
    name: String,

    /// Schema for the attributes of this relation
    schema: Schema,

    /// Collection of blocks on disk which contain records
    heap: Heap,
}

impl Relation {
    pub fn new(id: RelationIdT, name: String, schema: Schema, heap: Heap) -> Self {
        Self {
            id,
            name,
            schema,
            heap,
        }
    }
}
