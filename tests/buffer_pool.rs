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
        buffer_manager: BufferManager::new(
            DiskManager::new(common::TEST_DB_FILENAME),
            common::TEST_BUFFER_SIZE,
        ),
    }
}

#[test]
fn test_create_buffer_block() {
    let mut ctx = setup();

    let mut latches = Vec::new();
    for _ in 0..common::TEST_BUFFER_SIZE {
        let result = ctx.buffer_manager.create_block();
        assert!(result.is_ok());
        latches.push(result);
    }
    let result = ctx.buffer_manager.create_block();
    assert!(result.is_err());
}
