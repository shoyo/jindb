use crate::common::constants::RelationIdT;
use crate::relation::relation::Relation;
use crate::relation::schema::Schema;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

struct SystemCatalog {
    /// Mapping of relation IDs to relations
    relations: HashMap<RelationIdT, Relation>,

    /// Mapping of relation names to relation IDs
    relation_ids: HashMap<String, RelationIdT>,

    /// Next relation ID to be used
    next_relation_id: AtomicU32,
}

impl SystemCatalog {
    /// Create a new system catalog.
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            relation_ids: HashMap::new(),
            next_relation_id: AtomicU32::new(0),
        }
    }

    /// Create a new relation.
    pub fn create_relation(name: String, schema: Schema) -> Result<Relation, ()> {
        Err(())
    }

    /// Retrieve a relation by its ID.
    pub fn get_relation_by_id(id: RelationIdT) -> Option<Relation> {
        None
    }

    /// Retrieve a relation by its name.
    pub fn get_relation_by_name(name: String) -> Option<Relation> {
        None
    }
}
