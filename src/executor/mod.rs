/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jindb for more information about this project and its license.
 */

use crate::buffer::BufferManager;
use crate::catalog::SystemCatalog;
use crate::relation::record::Record;
use std::sync::{Arc, Mutex};

pub mod exec_insert;

/// The `executor` directory contains definitions for executor for a query plan tree.
/// Each executor type executes a certain operation (such as hash join, sequential scan, etc.)
/// for a corresponding plan node.
pub trait BaseExecutor {
    fn next() -> Option<Arc<Mutex<Record>>>;
}

/// All of the metadata required to execute a given query.
pub struct QueryMeta {
    system_catalog: Arc<SystemCatalog>,
    buffer_manager: Arc<BufferManager>,
    // TODO: Implement and add log and lock managers
}

impl QueryMeta {
    pub fn new(system_catalog: Arc<SystemCatalog>, buffer_manager: Arc<BufferManager>) -> Self {
        Self {
            system_catalog,
            buffer_manager,
        }
    }
}
