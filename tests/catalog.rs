/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::manager::BufferManager;
use jin::buffer::replacement::ReplacerAlgorithm;
use jin::concurrency::transaction_manager::TransactionManager;
use jin::disk::manager::DiskManager;
use jin::execution::system_catalog::SystemCatalog;
use jin::relation::attribute::Attribute;
use jin::relation::record::Record;
use jin::relation::schema::Schema;
use jin::relation::types::DataType;


use std::sync::Arc;
use std::thread;

mod common;

struct TestContext {
    txn_manager: TransactionManager,
    schema_1: Arc<Schema>,
    schema_2: Arc<Schema>,
    system_catalog: Arc<SystemCatalog>,
}

fn setup() -> TestContext {
    let buffer_manager = BufferManager::new(
        common::TEST_BUFFER_SIZE,
        DiskManager::new(common::TEST_DB_FILENAME),
        ReplacerAlgorithm::Slow,
    );

    let schema_1 = Arc::new(Schema::new(vec![
        Attribute::new("foo", DataType::Int, true, true, false),
        Attribute::new("bar", DataType::Boolean, false, false, false),
        Attribute::new("baz", DataType::Varchar, false, false, false),
    ]));

    let schema_2 = Arc::new(Schema::new(vec![
        Attribute::new("foobar", DataType::Int, true, true, false),
        Attribute::new("barbaz", DataType::Boolean, false, false, false),
    ]));

    TestContext {
        system_catalog: Arc::new(SystemCatalog::new(Arc::new(buffer_manager))),
        schema_1,
        schema_2,
        txn_manager: TransactionManager::new(),
    }
}

#[test]
fn test_create_relation() {
    let ctx = setup();

    let relation = ctx
        .system_catalog
        .create_relation("relation_1", ctx.schema_1.clone())
        .unwrap();
    assert_eq!(relation.get_id(), 0);

    let relation = ctx
        .system_catalog
        .create_relation("relation_2", ctx.schema_2.clone())
        .unwrap();
    assert_eq!(relation.get_id(), 1);
}

#[test]
fn test_get_relation() {
    let ctx = setup();
    let catalog1 = ctx.system_catalog.clone();
    let catalog2 = ctx.system_catalog.clone();

    // Create new relation.
    let relation = catalog1
        .create_relation("foo", ctx.schema_1.clone())
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

#[test]
fn test_insert_record() {
    let ctx = setup();

    // Create new relation.
    let relation = ctx
        .system_catalog
        .create_relation("foo", ctx.schema_1.clone())
        .unwrap();

    // Create a record for the newly created relation.
    // (Schema validation is done in Record constructor)
    let record = Record::new(
        vec![
            Some(Box::new(5)),
            Some(Box::new(false)),
            Some(Box::new("Hello!".to_string())),
        ],
        ctx.schema_1.clone(),
    )
    .unwrap();
    assert!(record.get_id().is_none());

    // Assert that the record can be inserted into the relation.
    let record_id = relation.insert_record(record).unwrap();
    assert_eq!(record_id.page_id, common::FIRST_RELATION_PAGE_ID);
    assert_eq!(record_id.slot_index, 0);
}

#[test]
fn test_insert_many_records() {
    let ctx = setup();

    // Create new relation.
    let relation = ctx
        .system_catalog
        .create_relation("foo", ctx.schema_1.clone())
        .unwrap();

    // Create a record for the newly created relation.
    let record = Record::new(
        vec![
            Some(Box::new(0)),
            Some(Box::new(true)),
            Some(Box::new(
                "abcdefghijklmnopqrstuvwxyz \
                abcdefghijklmnopqrstuvwxyz \
                abcdefghijklmnopqrstuvwxyz \
                abcdefghijklmnopqrstuvwxyz \
                abcdefghijklmnopqrstuvwxyz \
                abcdefghijklmnopqrstuvwxyz \
                abcdefghijklmnopqrstuvwxyz \
                abcdefghijklmnopqrstuvwxyz \
                abcdefghijklmnopqrstuvwxyz \
                abcdefghijklmnopqrstuvwxyz"
                    .to_string(),
            )),
        ],
        ctx.schema_1.clone(),
    )
    .unwrap();

    // Assert that several records can be inserted into the relation.
    for _ in 0..1000 {
        assert!(relation.insert_record(record.clone()).is_ok());
    }
}

#[test]
fn test_insert_many_records_in_parallel() {
    let ctx = setup();

    // Create two relations.
    let relation_1 = ctx
        .system_catalog
        .create_relation("relation_1", ctx.schema_1.clone())
        .unwrap();

    let relation_2 = ctx
        .system_catalog
        .create_relation("relation_2", ctx.schema_2.clone())
        .unwrap();

    // Create records for each newly created relation.
    let record_1 = Record::new(
        vec![
            Some(Box::new(0)),
            Some(Box::new(true)),
            Some(Box::new("Hello, World!!!!".to_string())),
        ],
        ctx.schema_1.clone(),
    )
    .unwrap();

    let record_2 = Record::new(
        vec![Some(Box::new(123456789)), Some(Box::new(true))],
        ctx.schema_2.clone(),
    )
    .unwrap();

    // Spin up several threads and simultaneously insert several records into both relations.
    for _ in 0..10 {
        let relation = relation_1.clone();
        let record = record_1.clone();
        thread::spawn(move || {
            assert!(relation.insert_record(record).is_ok());
        });
    }
    for _ in 0..10 {
        let relation = relation_2.clone();
        let record = record_2.clone();
        thread::spawn(move || {
            assert!(relation.insert_record(record).is_ok());
        });
    }
}

#[ignore]
#[test]
fn test_create_index() {
    assert!(false)
}
