/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::manager::BufferManager;
use jin::buffer::replacement::ReplacerAlgorithm;
use jin::disk::manager::DiskManager;
use jin::page::Page;
use std::sync::{mpsc, Arc};
use std::thread;

mod common;

fn setup() -> Arc<BufferManager> {
    Arc::new(BufferManager::new(
        common::TEST_BUFFER_SIZE,
        DiskManager::new(common::TEST_DB_FILENAME),
        ReplacerAlgorithm::Slow,
    ))
}

#[test]
fn test_create_buffer_page() {
    let manager = setup();

    // Create a page in the buffer manager.
    let frame_latch = manager.create_relation_page().unwrap();
    let frame = frame_latch.read().unwrap();

    // Assert that the created page is initialized as expected.
    assert!(frame.get_page().is_some());
    let page = frame.get_page().as_ref().unwrap();
    assert_eq!(page.get_id(), common::FIRST_RELATION_PAGE_ID);

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
    let mgr = setup();
    let mgr_1 = mgr.clone();
    let mgr_2 = mgr.clone();
    let (tx, rx) = mpsc::channel();

    // Create a page in the buffer manager.
    thread::spawn(move || {
        let frame_latch = mgr_1.create_relation_page().unwrap();
        let frame = frame_latch.read().unwrap();
        let page = frame.get_page().as_ref().unwrap();
        assert_eq!(page.get_id(), common::FIRST_RELATION_PAGE_ID);
        tx.send(page.get_id());
    })
    .join()
    .unwrap();

    // Fetch the same page in another thread.
    thread::spawn(move || {
        let page_id = rx.recv().unwrap();
        let frame_latch = mgr_2.fetch_page(page_id).unwrap();
        let frame = frame_latch.read().unwrap();
        assert!(frame.get_page().is_some());
        let page = frame.get_page().as_ref().unwrap();
        assert_eq!(page.get_id(), common::FIRST_RELATION_PAGE_ID);
    })
    .join()
    .unwrap();

    // Buffer now contains a single page with ID = 1.
    // Create TEST_BUFFER_SIZE more pages, which evicts page with ID = 1 with the Slow page
    // replacer.
    for _ in 0..common::TEST_BUFFER_SIZE {
        mgr.create_relation_page().unwrap();
    }

    for i in 0..common::TEST_BUFFER_SIZE + 1 {
        assert!(mgr.fetch_page(i).is_ok());
    }
    assert!(mgr.fetch_page(common::TEST_BUFFER_SIZE + 1).is_err());
}

#[test]
fn test_delete_buffer_page() {
    let mgr = setup();
    let mgr_2 = mgr.clone();
    let (tx, rx) = mpsc::channel();

    // First thread
    thread::spawn(move || {
        let frame_latch = mgr.create_relation_page().unwrap();
        let mut frame = frame_latch.write().unwrap();
        let page_id = frame.get_page().as_ref().unwrap().get_id();

        // Notify second thread to try to delete newly created page (should fail).
        tx.send(page_id);

        // Notify second thread to try again after unpinning created page (should pass).
        mgr.unpin_and_drop(frame);
        tx.send(page_id);
    });

    // Second thread
    thread::spawn(move || {
        // Receive notification from first thread to delete newly created page (should fail).
        let page_id = rx.recv().unwrap();
        let first_attempt = mgr_2.delete_page(page_id);
        assert!(first_attempt.is_err());

        // Receive notification from first thread to delete page again (should pass).
        let _ = rx.recv().unwrap();
        let second_attempt = mgr_2.delete_page(page_id);
        assert!(second_attempt.is_ok());
    });
}
