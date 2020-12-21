/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::buffer::manager::BufferManager;
use jin::disk::manager::DiskManager;

mod common;

struct TestContext {
    buffer_manager: BufferManager,
}

fn setup() -> TestContext {
    TestContext {
        buffer_manager: BufferManager::new(DiskManager::new(common::TEST_DB_FILENAME)),
    }
}

#[test]
fn test_create_buffer_block() {}
