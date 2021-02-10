/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::constants::{BufferFrameIdT, RelationIdT, DICTIONARY_PAGE_ID};

/// Constants used for testing
pub const TEST_DB_FILENAME: &str = "test_db.jin";
pub const TEST_BUFFER_SIZE: BufferFrameIdT = 64;
pub const FIRST_RELATION_PAGE_ID: RelationIdT = DICTIONARY_PAGE_ID + 1;
