/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::buffer::manager::BufferManager;
use crate::execution::plans::BasePlanNode;
use crate::execution::system_catalog::SystemCatalog;
use crate::execution::transaction::Transaction;
use crate::relation::record::Record;

pub mod aggregation;
pub mod delete;
pub mod index_scan;
pub mod insert;
pub mod limit;
pub mod nested_index_join;
pub mod nested_loop_join;
pub mod sequential_scan;
pub mod update;

pub trait BaseExecutor<'a> {
    fn next() -> &'a Record;
}

pub struct ExecutorMeta<'a> {
    transaction: &'a Transaction,
    system_catalog: &'a SystemCatalog,
    buffer_manager: &'a BufferManager,
    // TODO: Implement and add log and lock managers
}

impl<'a> ExecutorMeta<'a> {
    pub fn new(
        transaction: &'a Transaction,
        system_catalog: &'a SystemCatalog,
        buffer_manager: &'a BufferManager,
    ) -> Self {
        Self {
            transaction,
            system_catalog,
            buffer_manager,
        }
    }
}
