/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::RelationIdT;
use crate::execution::plans::QueryPlanNode;
use crate::relation::record::Record;
use crate::relation::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct InsertPlanNode {
    /// Relation affected by this insert plan.
    relation_id: RelationIdT,

    children: Vec<Arc<Box<dyn QueryPlanNode>>>,
    output_schema: Arc<Schema>,
}

impl QueryPlanNode for InsertPlanNode {
    fn next(&self) -> Option<Arc<Mutex<Record>>> {
        todo!()
    }

    fn get_children(&self) -> Arc<Vec<Arc<Box<dyn QueryPlanNode>>>> {
        todo!()
    }

    fn insert_child(&mut self, child: Arc<Box<dyn QueryPlanNode>>) {
        todo!()
    }

    fn get_output_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.output_schema)
    }
}
