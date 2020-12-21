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

mod common;

struct TestContext {
    txn_manager: TransactionManager,
    system_catalog: SystemCatalog,
}

fn setup() -> TestContext {
    let buffer_manager = BufferManager::new(
        DiskManager::new(common::TEST_DB_FILENAME),
        common::TEST_BUFFER_SIZE,
    );
    TestContext {
        system_catalog: SystemCatalog::new(buffer_manager),
        txn_manager: TransactionManager::new(),
    }
}

#[test]
fn test_create_table() {
    let mut context = setup();
    let table = context.system_catalog.create_relation(
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
