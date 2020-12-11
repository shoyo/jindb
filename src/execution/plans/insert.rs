/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::constants::RelationIdT;
use crate::execution::plans::{BasePlanNode, PlanMeta};

pub struct InsertPlanNode<'a> {
    meta: PlanMeta<'a>,
    relation_id: RelationIdT,
}

impl<'a> BasePlanNode<'a> for InsertPlanNode<'a> {
    fn get_child(&self, child_idx: u32) -> &'a dyn BasePlanNode<'a> {
        todo!()
    }
}
