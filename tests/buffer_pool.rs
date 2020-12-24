/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::manager::BufferManager;
use jin::disk::manager::DiskManager;
use std::thread;

mod common;

struct TestContext {
    buffer_manager: BufferManager,
}

fn setup() -> TestContext {
    TestContext {
        buffer_manager: BufferManager::new(
            DiskManager::new(common::TEST_DB_FILENAME),
            common::TEST_BUFFER_SIZE,
        ),
    }
}

#[test]
fn test_create_buffer_page() {
    let mut ctx = setup();

    // Create a page in the buffer manager.
    // Assert that the created page is initialized as expected.
    let result = ctx.buffer_manager.create_page();
    assert!(result.is_ok());

    let page_latch = result.unwrap();
    let page_option = page_latch.read().unwrap();
    assert!(page_option.is_some());

    let page = page_option.as_ref().unwrap();
    assert_eq!(page.id, 1);
    assert_eq!(page.pin_count, 1);
    assert_eq!(page.is_dirty, false);

    // Assert that new pages can't be created when the there are no open buffer frames and all
    // existing pages are pinned. (i.e. all held latches are still in scope)
    let mut latches = Vec::new();
    for _ in 0..common::TEST_BUFFER_SIZE - 1 {
        let result = ctx.buffer_manager.create_page();
        assert!(result.is_ok());
        latches.push(result.unwrap());
    }
    let result = ctx.buffer_manager.create_page();
    assert!(result.is_err());
}

#[test]
fn test_fetch_buffer_page() {
    let mut ctx = setup();

    // Create a page in the buffer manager.
    let page_latch = ctx.buffer_manager.create_page().unwrap();
    let page_option = page_latch.read().unwrap();
    let page = page_option.as_ref().unwrap();
    let page_id = page.id;

    // Fetch the same page in another thread.
    // Assert that the operation is successful and correct.
    thread::spawn(move || {
        let result = ctx.buffer_manager.fetch_page(page_id);
        assert!(result.is_ok());

        let page_latch = result.unwrap();
        let page_option = page_latch.read().unwrap();
        assert!(page_option.is_some());

        let page = page_option.as_ref().unwrap();
        assert_eq!(page.id, page_id);
        assert_eq!(page.pin_count, 2);
        assert_eq!(page.is_dirty, false);
    });
}

#[test]
fn test_delete_buffer_page() {
    let mut ctx = setup();

    // Create a page in the buffer manager.
    let page_latch = ctx.buffer_manager.create_page().unwrap();
    let page_option = page_latch.read().unwrap();
    let page = page_option.as_ref().unwrap();
    let page_id = page.id;

    // Assert that the page cannot be deleted while pinned.
    thread::spawn(move || {
        let result = ctx.buffer_manager.delete_page(page_id);
        assert!(result.is_err());
    });

    // Assert that the page can be deleted when its pin count is zero.
    // let result = ctx.buffer_manager.delete_page(page_id);
}
