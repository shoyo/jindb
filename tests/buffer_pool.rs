/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::eviction_policies::PolicyVariant;
use jin::buffer::manager::BufferManager;
use jin::disk::manager::DiskManager;
use jin::page::Page;
use std::sync::{mpsc, Arc};
use std::thread;

mod common;

fn setup() -> Arc<BufferManager> {
    Arc::new(BufferManager::new(
        common::TEST_BUFFER_SIZE,
        DiskManager::new(common::TEST_DB_FILENAME),
        PolicyVariant::Slow,
    ))
}

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

#[test]
fn test_fetch_buffer_page() {
    let manager = setup();
    let managerc = manager.clone();
    let (tx, rx) = mpsc::channel();

    // Create a page in the buffer manager.
    thread::spawn(move || {
        let page_latch = manager.create_relation_page().unwrap();
        let frame = page_latch.read().unwrap();
        let page = frame.as_ref().unwrap();
        assert_eq!(page.get_id(), 1);
        tx.send(page.get_id());
    });

    // Fetch the same page in another thread.
    thread::spawn(move || {
        let page_id = rx.recv().unwrap();
        let page_latch = managerc.fetch_page(page_id).unwrap();
        let frame = page_latch.read().unwrap();
        assert!(frame.is_some());
        let page = frame.as_ref().unwrap();
        assert_eq!(page.get_id(), 1);

        let result = managerc.fetch_page(2);
        assert!(result.is_err());
    });
}

#[ignore]
#[test]
fn test_delete_buffer_page() {
    let _ctx = setup();

    // Create a page in the buffer manager.

    // Assert that the page cannot be deleted while pinned.

    // Assert that the page can be deleted when its pin count is zero.
}
