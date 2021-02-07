/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::replacement::ReplacerAlgorithm;
use jin::buffer::BufferManager;
use jin::catalog::SystemCatalog;
use jin::disk::DiskManager;
use jin::relation::record::{Record, RecordId};
use jin::relation::types::{DataType, InnerValue};
use jin::relation::Attribute;
use jin::relation::Schema;

use jin::relation::heap::HeapError;
use std::sync::Arc;
use std::thread;

mod constants;

struct TestContext {
    schema_1: Arc<Schema>,
    schema_2: Arc<Schema>,
    system_catalog: Arc<SystemCatalog>,
}

fn setup() -> TestContext {
    let buffer_manager = BufferManager::new(
        constants::TEST_BUFFER_SIZE,
        DiskManager::new(constants::TEST_DB_FILENAME),
        ReplacerAlgorithm::Slow,
    );

    let schema_1 = Arc::new(Schema::new(vec![
        Attribute::new("foo", DataType::Int, true, true, true),
        Attribute::new("bar", DataType::Boolean, false, false, true),
        Attribute::new("baz", DataType::Varchar, false, false, true),
    ]));

    let schema_2 = Arc::new(Schema::new(vec![
        Attribute::new("foobar", DataType::Int, true, true, false),
        Attribute::new("barbaz", DataType::Boolean, false, false, false),
    ]));

    TestContext {
        system_catalog: Arc::new(SystemCatalog::new(Arc::new(buffer_manager))),
        schema_1,
        schema_2,
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
    let record_id = relation.insert(record).unwrap();
    assert_eq!(record_id.page_id, constants::FIRST_RELATION_PAGE_ID);
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
    for _ in 0..20 {
        assert!(relation.insert(record.clone()).is_ok());
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
            Some(Box::new("Hello, World!".to_string())),
        ],
        ctx.schema_1.clone(),
    )
    .unwrap();

    let record_2 = Record::new(
        vec![Some(Box::new(123456789_i32)), Some(Box::new(false))],
        ctx.schema_2.clone(),
    )
    .unwrap();

    let num_threads = 20;
    let num_inserts_per_thread = 100;
    let mut handles = Vec::with_capacity(num_threads);

    // Spin up several threads and simultaneously insert several records into both relations.
    for _ in 0..num_threads / 2 {
        let relation = relation_1.clone();
        let record = record_1.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..num_inserts_per_thread {
                relation.insert(record.clone()).unwrap();
            }
        }));
    }
    for _ in 0..num_threads / 2 {
        let relation = relation_2.clone();
        let record = record_2.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..num_inserts_per_thread {
                relation.insert(record.clone()).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_read_record() {
    let ctx = setup();

    // Create a relation and insert a record.
    let relation = ctx
        .system_catalog
        .create_relation("foo", ctx.schema_1.clone())
        .unwrap();
    let record = Record::new(
        vec![Some(Box::new(54321_i32)), Some(Box::new(false)), None],
        ctx.schema_1.clone(),
    )
    .unwrap();
    let rid = relation.insert(record).unwrap();

    // Assert that read values are correct.
    let dne = RecordId {
        page_id: rid.page_id,
        slot_index: rid.slot_index + 1,
    };

    let result = relation.read(rid);
    assert!(result.is_ok());
    assert!(relation.read(dne).is_err());

    let record = result.unwrap();

    let value = record
        .get_value(0, ctx.schema_1.clone())
        .unwrap()
        .unwrap()
        .get_inner();
    assert_eq!(value, InnerValue::Int(54321));

    let value = record
        .get_value(1, ctx.schema_1.clone())
        .unwrap()
        .unwrap()
        .get_inner();
    assert_eq!(value, InnerValue::Boolean(false));

    let value = record.get_value(2, ctx.schema_1.clone()).unwrap();
    assert!(value.is_none());
}

#[test]
fn test_update_record() {
    let ctx = setup();

    // Create a relation and insert a record.
    let relation = ctx
        .system_catalog
        .create_relation("foo", ctx.schema_1.clone())
        .unwrap();
    let record = Record::new(
        vec![
            Some(Box::new(54321)),
            Some(Box::new(false)),
            Some(Box::new("Hello, World!".to_string())),
        ],
        ctx.schema_1.clone(),
    )
    .unwrap();
    let record_id = relation.insert(record).unwrap();

    // Update the existing record (to a larger record).
    let update = Record::new(
        vec![
            Some(Box::new(12345)),
            None,
            Some(Box::new("Hello, World! Hello, World!".to_string())),
        ],
        ctx.schema_1.clone(),
    )
    .unwrap();
    let result = relation.update(update, record_id);
    assert!(result.is_ok());

    // Assert that the record is correctly updated.
    let record = relation.read(record_id).unwrap();
    assert_eq!(record.get_id().unwrap(), record_id);

    let value = record
        .get_value(0, ctx.schema_1.clone())
        .unwrap()
        .unwrap()
        .get_inner();
    assert_eq!(value, InnerValue::Int(12345));

    let value = record.get_value(1, ctx.schema_1.clone()).unwrap();
    assert!(value.is_none());

    let value = record
        .get_value(2, ctx.schema_1.clone())
        .unwrap()
        .unwrap()
        .get_inner();
    assert_eq!(value, InnerValue::Varchar("Hello!".to_string()));
}

#[test]
fn test_delete_record() {
    let ctx = setup();

    // Create a relation and insert a record.
    let relation = ctx
        .system_catalog
        .create_relation("foo", ctx.schema_1.clone())
        .unwrap();
    let record = Record::new(
        vec![
            Some(Box::new(54321)),
            Some(Box::new(false)),
            Some(Box::new("Hello, World!".to_string())),
        ],
        ctx.schema_1.clone(),
    )
    .unwrap();
    let record_id = relation.insert(record).unwrap();

    // Flag and delete the existing record.
    let result = relation.flag_delete(record_id);
    assert!(result.is_ok());

    assert!(false);
}

#[test]
fn test_flag_delete_then_read_record() {
    let ctx = setup();

    let relation = ctx
        .system_catalog
        .create_relation("foo", ctx.schema_1.clone())
        .unwrap();
    let record = Record::new(
        vec![
            Some(Box::new(54321)),
            Some(Box::new(false)),
            Some(Box::new("Hello, World!".to_string())),
        ],
        ctx.schema_1.clone(),
    )
    .unwrap();
    let record_id = relation.insert(record).unwrap(); // Create a relation and insert a record.

    // Flag the existing record, then attempt to read it.
    relation.flag_delete(record_id).unwrap();
    assert_eq!(
        relation.read(record_id).unwrap_err(),
        HeapError::RecordDeleted
    );

    assert!(false);
}

#[ignore]
#[test]
fn test_create_index() {
    assert!(false)
}
