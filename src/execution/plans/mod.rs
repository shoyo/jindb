/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::relation::schema::Schema;

pub mod insert;

pub trait BasePlanNode<'a> {
    /// Return a reference to the child node of this plan node at the given index.
    fn get_child(&self, child_idx: u32) -> &'a dyn BasePlanNode<'a>;
}

pub struct PlanMeta<'a> {
    /// Child nodes of this plan node.
    pub children: Vec<&'a dyn BasePlanNode<'a>>,

    /// Schema of records outputted by this plan node.
    pub output_schema: &'a Schema,
}
