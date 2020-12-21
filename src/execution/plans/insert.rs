/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::RelationIdT;
use crate::execution::plans::{AbstractPlanNode, PlanNode};
use crate::relation::record::Record;
use crate::relation::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct InsertPlanNode<'a> {
    /// Relation affected by this insert plan.
    relation_id: RelationIdT,

    children: Vec<PlanNode<'a>>,
    output_schema: &'a Schema,
}

impl<'a> AbstractPlanNode<'a> for InsertPlanNode<'a> {
    fn next(&self) -> Option<Arc<Mutex<Record>>> {
        todo!()
    }

    fn get_children(&'a self) -> &'a Vec<PlanNode<'a>> {
        &self.children
    }

    fn get_output_schema(&'a self) -> &'a Schema {
        self.output_schema
    }
}
