/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::constants::{BufferFrameIdT, RelationIdT, CATALOG_ROOT_ID};

/// Constants used for testing
pub const TEST_DB_FILENAME: &str = "test_db.jin";
pub const TEST_BUFFER_SIZE: BufferFrameIdT = 64;
pub const FIRST_RELATION_PAGE_ID: RelationIdT = CATALOG_ROOT_ID + 1;
