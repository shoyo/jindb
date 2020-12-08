use super::heap::Heap;
use super::schema::Schema;
use crate::common::constants::RelationIdT;
use crate::relation::record::Record;
use std::sync::{Arc, Mutex};

pub type RelationGuard = Arc<Mutex<Relation>>;

/// Database relation (i.e. table) represented on disk.
pub struct Relation {
    /// Unique ID for this relation
    pub id: RelationIdT,

    /// User-defined name for this relation
    pub name: String,

    /// Schema for the attributes of this relation
    pub schema: Schema,

    /// Collection of blocks on disk which contain records
    pub heap: Heap,
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
