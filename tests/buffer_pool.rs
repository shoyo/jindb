/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::eviction_policies::PolicyVariant;
use jin::buffer::manager::BufferManager;
use jin::disk::manager::DiskManager;
use jin::page::Page;
use std::sync::Arc;
use std::thread;

mod common;

fn setup() -> Arc<BufferManager> {
    Arc::new(BufferManager::new(
        common::TEST_BUFFER_SIZE,
        DiskManager::new(common::TEST_DB_FILENAME),
        PolicyVariant::Slow,
    ))
}

#[ignore]
#[test]
fn test_create_buffer_page() {
    let manager = setup();

    // Create a page in the buffer manager.
    let page_latch = manager.create_relation_page().unwrap();
    let frame = page_latch.read().unwrap();

    // Assert that the created page is initialized as expected.
    assert!(frame.is_some());
    let page = frame.as_ref().unwrap();
    assert_eq!(page.get_id(), 1);
    assert_eq!(page.get_pin_count(), 1);
    assert_eq!(page.is_dirty(), false);

    // Assert that new pages can't be created when the there are no open buffer frames and all
    // existing pages are pinned.
    let mut latches = Vec::new();
    for _ in 1..common::TEST_BUFFER_SIZE {
        latches.push(manager.create_relation_page().unwrap());
    }
    assert!(manager.create_relation_page().is_err());
}

#[ignore]
#[test]
fn test_fetch_buffer_page() {
    let mut ctx = setup();

    // Create a page in the buffer manager.

    // Fetch the same page in another thread.
    // Assert that the operation is successful and correct.
}

#[ignore]
#[test]
fn test_delete_buffer_page() {
    let mut ctx = setup();

    // Create a page in the buffer manager.

    // Assert that the page cannot be deleted while pinned.

    // Assert that the page can be deleted when its pin count is zero.
}
