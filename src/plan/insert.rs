/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jindb for more information about this project and its license.
 */

use crate::constants::RelationIdT;
use crate::plan::{PlanVariant, QueryPlanNode};
use crate::relation::record::Record;
use crate::relation::Schema;
use std::sync::{Arc, Mutex, RwLock};

pub struct InsertPlanNode {
    /// Relation affected by this insert plan.
    relation_id: RelationIdT,

    children: Arc<RwLock<Vec<Arc<Box<dyn QueryPlanNode>>>>>,
    output_schema: Arc<Schema>,
}

impl InsertPlanNode {
    pub fn new(relation_id: RelationIdT, output_schema: Arc<Schema>) -> Self {
        Self {
            relation_id,
            children: Arc::new(RwLock::new(Vec::new())),
            output_schema,
        }
    }
}

impl QueryPlanNode for InsertPlanNode {
    fn next(&self) -> Option<Arc<Mutex<Record>>> {
        todo!()
    }

    fn get_children(&self) -> Arc<RwLock<Vec<Arc<Box<dyn QueryPlanNode>>>>> {
        Arc::clone(&self.children)
    }

    fn get_output_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.output_schema)
    }

    fn get_variant(&self) -> PlanVariant {
        PlanVariant::Insert
    }
}
