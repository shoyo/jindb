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
