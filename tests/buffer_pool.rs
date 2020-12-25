/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::manager::BufferManager;
use jin::disk::manager::DiskManager;
use jin::page::Page;
use std::sync::Arc;
use std::thread;

mod common;

struct TestContext {
    buffer_manager: Arc<BufferManager>,
}

fn setup() -> TestContext {
    TestContext {
        buffer_manager: Arc::new(BufferManager::new(
            common::TEST_BUFFER_SIZE,
            DiskManager::new(common::TEST_DB_FILENAME),
        )),
    }
}

#[test]
fn test_create_buffer_page() {
    let mut ctx = setup();

    // Create a page in the buffer manager.
    // Assert that the created page is initialized as expected.

    // Assert that new pages can't be created when the there are no open buffer frames and all
    // existing pages are pinned. (i.e. all held latches are still in scope)
}

#[test]
fn test_fetch_buffer_page() {
    let mut ctx = setup();

    // Create a page in the buffer manager.

    // Fetch the same page in another thread.
    // Assert that the operation is successful and correct.
}

#[test]
fn test_delete_buffer_page() {
    let mut ctx = setup();

    // Create a page in the buffer manager.

    // Assert that the page cannot be deleted while pinned.

    // Assert that the page can be deleted when its pin count is zero.
}
