/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::manager::BufferManager;
use jin::buffer::replacement::ReplacerAlgorithm;
use jin::concurrency::transaction_manager::TransactionManager;
use jin::disk::manager::DiskManager;
use jin::execution::system_catalog::SystemCatalog;
use jin::relation::attribute::{Attribute, DataType};
use jin::relation::schema::Schema;
use std::sync::Arc;
use std::thread;

mod common;

struct TestContext {
    txn_manager: TransactionManager,
    system_catalog: Arc<SystemCatalog>,
}

fn setup() -> TestContext {
    let buffer_manager = BufferManager::new(
        common::TEST_BUFFER_SIZE,
        DiskManager::new(common::TEST_DB_FILENAME),
        ReplacerAlgorithm::Slow,
    );
    TestContext {
        system_catalog: Arc::new(SystemCatalog::new(Arc::new(buffer_manager))),
        txn_manager: TransactionManager::new(),
    }
}

#[test]
fn test_create_relation() {
    let mut context = setup();

    let relation = context
        .system_catalog
        .create_relation(
            "Students",
            Schema::new(vec![
                Attribute::new("id", DataType::Int, true, true, false),
                Attribute::new("name", DataType::Varchar, false, false, false),
            ]),
        )
        .unwrap();
    assert_eq!(relation.get_id(), 0);

    let relation = context
        .system_catalog
        .create_relation(
            "Restaurants",
            Schema::new(vec![
                Attribute::new("id", DataType::Int, true, true, false),
                Attribute::new("name", DataType::Varchar, false, false, false),
            ]),
        )
        .unwrap();
    assert_eq!(relation.get_id(), 1);
}

#[test]
fn test_get_relation() {
    let mut context = setup();
    let catalog1 = context.system_catalog.clone();
    let catalog2 = context.system_catalog.clone();

    // Create new relation.
    let relation = context
        .system_catalog
        .create_relation("foo", Schema::new(vec![]))
        .unwrap();

    let id = relation.get_id();
    let name = relation.get_name().to_string();
    let name_c = name.clone();

    // Fetch relation by id and assert that fetched relation is correct.
    thread::spawn(move || {
        let result = catalog1.get_relation_by_id(id);
        assert!(result.is_some());

        let relation = result.unwrap();
        assert_eq!(relation.get_id(), id);
        assert_eq!(relation.get_name(), &name);
    });

    // Fetch relation by name and assert that fetched relation is correct.
    thread::spawn(move || {
        let result = catalog2.get_relation_by_name(&name_c);
        assert!(result.is_some());

        let relation = result.unwrap();
        assert_eq!(relation.get_id(), id);
        assert_eq!(relation.get_name(), &name_c);
    });
}

#[ignore]
#[test]
fn test_insert_update_delete_tuple() {
    assert!(false);
}

#[ignore]
#[test]
fn test_create_index() {
    assert!(false)
}
