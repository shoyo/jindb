use crate::buffer::manager::BufferManager;
use crate::common::constants::RelationIdT;
use crate::relation::heap::Heap;
use crate::relation::relation::Relation;
use crate::relation::schema::Schema;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

type RelationGuard = Arc<Mutex<Relation>>;

struct SystemCatalog {
    /// Mapping of relation IDs to relations
    relations: HashMap<RelationIdT, RelationGuard>,

    /// Mapping of relation names to relation IDs
    relation_ids: HashMap<String, RelationIdT>,

    /// Next relation ID to be used
    next_relation_id: AtomicU32,

    /// Buffer manager instance backng the relations in this catalog
    buffer_manager: BufferManager,
}

impl SystemCatalog {
    /// Create a new system catalog.
    pub fn new(buffer_manager: BufferManager) -> Self {
        Self {
            relations: HashMap::new(),
            relation_ids: HashMap::new(),
            next_relation_id: AtomicU32::new(0),
            buffer_manager,
        }
    }

    /// Create a new relation.
    pub fn create_relation(
        &mut self,
        name: String,
        schema: Schema,
    ) -> Result<RelationGuard, String> {
        let heap = match Heap::new(&mut self.buffer_manager) {
            Ok(heap) => heap,
            Err(e) => return Err(e),
        };
        let relation_id = self.get_next_relation_id();
        let relation = Relation::new(relation_id, name.clone(), schema, heap);
        self.relation_ids.insert(name, relation_id);

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
        self.next_relation_id.fetch_add(1, Ordering::SeqCst)
    }
}