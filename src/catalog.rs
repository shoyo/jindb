/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jindb for more information about this project and its license.
 */

use crate::buffer::{BufferError, BufferManager};
use crate::constants::RelationIdT;
use crate::relation::heap::Heap;
use crate::relation::Relation;
use crate::relation::Schema;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

/// The system catalog maintains metadata about relations in the database.
pub struct SystemCatalog {
    /// Mapping of relation IDs to relations
    relations: Arc<RwLock<HashMap<RelationIdT, Arc<Relation>>>>,

    /// Mapping of relation names to relation IDs
    relation_ids: Arc<RwLock<HashMap<String, RelationIdT>>>,

    /// Next relation ID to be used
    next_relation_id: AtomicU32,

    /// Buffer manager instance backing the relations in this catalog
    buffer_manager: Arc<BufferManager>,
}

impl SystemCatalog {
    /// Create a new system catalog.
    pub fn new(buffer_manager: Arc<BufferManager>) -> Self {
        Self {
            relations: Arc::new(RwLock::new(HashMap::new())),
            relation_ids: Arc::new(RwLock::new(HashMap::new())),
            next_relation_id: AtomicU32::new(0),
            buffer_manager,
        }
    }

    /// Initialize a new relation and return a protected reference.
    pub fn create_relation(
        &self,
        name: &str,
        schema: Arc<Schema>,
    ) -> Result<Arc<Relation>, BufferError> {
        // Initialize a new database heap.
        let heap = Arc::new(Heap::new(self.buffer_manager.clone())?);

        // Create a new relation with the given name, schema, and newly initialized heap.
        let relation_id = self.get_next_relation_id();
        let relation = Arc::new(Relation::new(relation_id, name.to_string(), schema, heap));

        // Lock and update the relation_ids and relations table.
        let mut relation_ids = self.relation_ids.write().unwrap();
        let mut relations = self.relations.write().unwrap();
        relation_ids.insert(name.to_string(), relation_id);
        relations.insert(relation_id, relation.clone());

        // Return a reference to the relation.
        Ok(relation)
    }

    /// Lookup a relation by its name and return a protected reference.
    /// Return None if a relation does exist in the database with the given name.
    pub fn get_relation(&self, name: &str) -> Option<Arc<Relation>> {
        let relation_ids = self.relation_ids.read().unwrap();
        match relation_ids.get(name) {
            Some(&id) => self.get_relation_by_id(id),
            None => None,
        }
    }

    /// Lookup a relation by its ID and return a protected reference.
    /// Return None if a relation does not exist in the database with the given ID.
    pub fn get_relation_by_id(&self, id: RelationIdT) -> Option<Arc<Relation>> {
        let relations = self.relations.read().unwrap();
        match relations.get(&id) {
            Some(relation) => Some(relation.clone()),
            None => None,
        }
    }

    /// Return the next relation ID and atomically increment the counter.
    fn get_next_relation_id(&self) -> u32 {
        // Note: .fetch_add() increments the value and returns the PREVIOUS value
        self.next_relation_id.fetch_add(1, Ordering::SeqCst)
    }
}
