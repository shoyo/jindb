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
fn test_create_buffer_block() {
    let mut ctx = setup();

    // Create a block in the buffer manager.
    // Assert that the created block is initialized as expected.
    let result = ctx.buffer_manager.create_block();
    assert!(result.is_ok());

    let block_latch = result.unwrap();
    let block_option = block_latch.read().unwrap();
    assert!(block_option.is_some());

    let block = block_option.as_ref().unwrap();
    assert_eq!(block.id, 1);
    assert_eq!(block.pin_count, 1);
    assert_eq!(block.is_dirty, false);

    // Assert that new blocks can't be created when the there are no open buffer frames and all
    // existing blocks are pinned. (i.e. all held latches are still in scope)
    let mut latches = Vec::new();
    for _ in 0..common::TEST_BUFFER_SIZE - 1 {
        let result = ctx.buffer_manager.create_block();
        assert!(result.is_ok());
        latches.push(result.unwrap());
    }
    let result = ctx.buffer_manager.create_block();
    assert!(result.is_err());
}

#[test]
fn test_fetch_buffer_block() {
    let mut ctx = setup();

    // Create a block in the buffer manager.
    let block_latch = ctx.buffer_manager.create_block().unwrap();
    let block_option = block_latch.read().unwrap();
    let block = block_option.as_ref().unwrap();
    let block_id = block.id;

    // Fetch the same block in another thread.
    // Assert that the operation is successful and correct.
    thread::spawn(move || {
        let result = ctx.buffer_manager.fetch_block(block_id);
        assert!(result.is_ok());

        let block_latch = result.unwrap();
        let block_option = block_latch.read().unwrap();
        assert!(block_option.is_some());

        let block = block_option.as_ref().unwrap();
        assert_eq!(block.id, block_id);
        assert_eq!(block.pin_count, 2);
        assert_eq!(block.is_dirty, false);
    });
}

#[test]
fn test_delete_buffer_block() {
    let mut ctx = setup();

    // Create a block in the buffer manager.
    let block_latch = ctx.buffer_manager.create_block().unwrap();
    let block_option = block_latch.read().unwrap();
    let block = block_option.as_ref().unwrap();
    let block_id = block.id;

    // Assert that the block cannot be deleted while pinned.
    thread::spawn(move || {
        let result = ctx.buffer_manager.delete_block(block_id);
        assert!(result.is_err());
    });

    // Assert that the block can be deleted when its pin count is zero.
    let result = ctx.buffer_manager.delete_block(block_id);
}
