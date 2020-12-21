/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::relation::record::Record;
use crate::relation::schema::Schema;
use std::sync::{Arc, Mutex};

pub mod insert;

/// An abstract node in a query plan.
type PlanNode<'a> = Arc<Mutex<dyn AbstractPlanNode<'a>>>;

/// A public trait for query plan nodes. Nodes are connected as a directed acyclic graph.
pub trait AbstractPlanNode<'a> {
    /// Return the next record to be processed.
    /// This method is invoked repeatedly by the parent node during query execution.
    fn next(&self) -> Option<Arc<Mutex<Record>>>;

    /// Return all child nodes.
    fn get_children(&'a self) -> &'a Vec<PlanNode<'a>>;

    /// Return the n-th child node.
    fn get_nth_child(&'a self, idx: usize) -> Option<PlanNode<'a>> {
        let children = self.get_children();
        if idx >= children.len() {
            return None;
        }
        Some(children[idx].clone())
    }

    /// Return the schema of the records outputted by this node.
    fn get_output_schema(&'a self) -> &'a Schema;
}
