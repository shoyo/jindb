/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::manager::BufferManager;
use jin::concurrency::transaction_manager::TransactionManager;
use jin::disk::manager::DiskManager;
use jin::execution::system_catalog::SystemCatalog;
use jin::relation::attribute::{Attribute, DataType};
use jin::relation::schema::Schema;
use std::sync::Arc;

struct ExecutionTestContext {
    disk_manager: DiskManager,
    buffer_manager: BufferManager,
    txn_manager: TransactionManager,
}

fn setup() -> ExecutionTestContext {}

#[test]
fn test_create_table() {
    let disk_manager = DiskManager::new("test_db.jin");
    let buffer_manager = BufferManager::new(disk_manager);
    let mut catalog = SystemCatalog::new(buffer_manager);
    let table = catalog.create_relation(
        "Students",
        Schema::new(
            (vec![
                Attribute::new("id", DataType::Varchar, true, true, false),
                Attribute::new("name", DataType::Varchar, false, false, false),
                Attribute::new("school", DataType::Varchar, false, false, false),
                Attribute::new("grade", DataType::TinyInt, false, false, false),
            ]),
        ),
    );
}

#[test]
fn test_insert_update_delete_tuple() {
    assert!(false);
}

#[test]
fn test_create_index() {
    assert!(false)
}
