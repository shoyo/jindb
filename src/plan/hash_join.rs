/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::plan::{PlanVariant, QueryPlanNode};
use crate::relation::record::Record;
use crate::relation::Schema;
use std::sync::{Arc, Mutex, RwLock};

pub struct HashJoinPlanNode {
    children: Arc<RwLock<Vec<Arc<Box<dyn QueryPlanNode>>>>>,
    output_schema: Arc<Schema>,
}

impl HashJoinPlanNode {
    pub fn new(output_schema: Arc<Schema>) -> Self {
        Self {
            children: Arc::new(RwLock::new(Vec::new())),
            output_schema,
        }
    }
}

impl QueryPlanNode for HashJoinPlanNode {
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
        PlanVariant::HashJoin
    }
}
