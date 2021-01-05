/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::manager::{BufferManager, NoBufFrameErr};
use crate::common::RelationIdT;
use crate::relation::heap::Heap;
use crate::relation::relation::{Relation, RelationGuard};
use crate::relation::schema::Schema;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

pub struct SystemCatalog {
    /// Mapping of relation IDs to relations
    relations: HashMap<RelationIdT, RelationGuard>,

    /// Mapping of relation names to relation IDs
    relation_ids: HashMap<String, RelationIdT>,

    /// Next relation ID to be used
    next_relation_id: AtomicU32,

    /// Buffer manager instance backing the relations in this catalog
    buffer_manager: Arc<BufferManager>,
}

impl SystemCatalog {
    /// Create a new system catalog.
    pub fn new(buffer_manager: BufferManager) -> Self {
        Self {
            relations: HashMap::new(),
            relation_ids: HashMap::new(),
            next_relation_id: AtomicU32::new(0),
            buffer_manager: Arc::new(buffer_manager),
        }
    }

    /// Create a new relation.
    pub fn create_relation(
        &mut self,
        name: &str,
        schema: Schema,
    ) -> Result<RelationGuard, NoBufFrameErr> {
        let heap = Heap::new(self.buffer_manager.clone())?;
        let relation_id = self.get_next_relation_id();
        let relation = Relation::new(relation_id, name.to_string(), schema, heap);
        self.relation_ids.insert(name.to_string(), relation_id);

        let guard = Arc::new(Mutex::new(relation));
        self.relations.insert(relation_id, guard.clone());
        Ok(guard)
    }

    /// Retrieve a relation by its ID.
    pub fn get_relation_by_id(&self, id: RelationIdT) -> Option<RelationGuard> {
        match self.relations.get(&id) {
            Some(guard) => Some(guard.clone()),
            None => None,
        }
    }

    /// Retrieve a relation by its name.
    pub fn get_relation_by_name(&self, name: String) -> Option<RelationGuard> {
        let id = match self.relation_ids.get(&name) {
            Some(&id) => id,
            None => return None,
        };
        self.get_relation_by_id(id)
    }

    /// Return the next relation ID and atomically increment the counter.
    fn get_next_relation_id(&mut self) -> u32 {
        // Note: .fetch_add() increments the value and returns the PREVIOUS value
        self.next_relation_id.fetch_add(1, Ordering::SeqCst)
    }
}
