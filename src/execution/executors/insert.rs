/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::execution::executors::{BaseExecutor, QueryMeta};
use crate::execution::plans::insert::InsertPlanNode;
use crate::relation::record::Record;
use std::sync::{Arc, Mutex};

/// An executor for insert operations in the database.
pub struct InsertExecutor {
    /// Metadata for this executor
    meta: QueryMeta,

    /// Insert plan node to be executed
    node: InsertPlanNode,
}

impl InsertExecutor {
    pub fn new(meta: QueryMeta, node: InsertPlanNode) -> Self {
        Self { meta, node }
    }
}

impl BaseExecutor for InsertExecutor {
    fn next() -> Option<Arc<Mutex<Record>>> {
        todo!()
    }
}
