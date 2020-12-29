/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

/// The `plans` directory contains definitions for nodes on a query plan tree.
/// Each node represents a single operation (such as hash join, sequential scan, etc.) on a
/// collection of database records.
/// During execution, an executor repeatedly calls `next()` on a node to obtain the processed
/// records of each plan node. Parent nodes receive the records produced by all of its child nodes
/// (follows the "Volcano Model").
use crate::relation::record::Record;
use crate::relation::schema::Schema;
use std::sync::{Arc, Mutex, RwLock};

pub mod aggregation;
pub mod hash_join;
pub mod insert;
pub mod seq_scan;

/// A public trait for query plan nodes.
pub trait QueryPlanNode {
    /// Return the next record to be processed.
    /// This method is invoked repeatedly by the parent node during query execution.
    fn next(&self) -> Option<Arc<Mutex<Record>>>;

    /// Return all child nodes.
    fn get_children(&self) -> Arc<RwLock<Vec<Arc<Box<dyn QueryPlanNode>>>>>;

    /// Return the n-th child node.
    fn get_nth_child(&self, idx: usize) -> Option<Arc<Box<dyn QueryPlanNode>>> {
        let rwlock = self.get_children();
        let children = rwlock.read().unwrap();
        if idx >= children.len() {
            return None;
        }
        Some(Arc::clone(&children[idx]))
    }

    /// Append a child node.
    fn insert_child(&mut self, child: Arc<Box<dyn QueryPlanNode>>) {
        let children = self.get_children();
        children.write().unwrap().push(child);
    }

    /// Return the schema of the records outputted by this node.
    fn get_output_schema(&self) -> Arc<Schema>;

    /// Return the variant of this plan node.
    fn get_variant(&self) -> PlanVariant;
}

#[derive(Clone, Copy)]
pub enum PlanVariant {
    Aggregation,
    Insert,
    HashJoin,
    SeqScan,
}
