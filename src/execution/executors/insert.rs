/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::execution::executors::{BaseExecutor, ExecutorMeta};
use crate::execution::plans::insert::InsertPlanNode;
use crate::relation::record::Record;
use std::sync::{Arc, Mutex};

/// An executor for insert operations in the database.
pub struct InsertExecutor<'a> {
    /// Metadata for this executor
    meta: ExecutorMeta<'a>,

    /// Insert plan node to be executed
    node: InsertPlanNode<'a>,
}

impl<'a> InsertExecutor<'a> {
    pub fn new(meta: ExecutorMeta<'a>, node: InsertPlanNode<'a>) -> Self {
        Self { meta, node }
    }
}

impl<'a> BaseExecutor<'a> for InsertExecutor<'a> {
    fn next() -> Option<Arc<Mutex<Record>>> {
        todo!()
    }
}
