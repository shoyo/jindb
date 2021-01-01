/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::BufferFrameIdT;

/// An eviction policy trait for the database buffer.
/// The policy used decides which page in the buffer is evicted when the
/// buffer is full and a new page is requested.
///
/// As a general rule, pages that are pinned to the buffer are never
/// evicted. This means that there are cases where no pages can be
/// removed, and the eviction operation fails.

pub trait Policy {
    /// Create a new instance of a replacer that behaves according to the
    /// eviction policy.
    fn new() -> Self;

    /// Evict a page from the buffer according to the eviction policy and
    /// return the corresponding frame ID.
    fn evict(&mut self) -> Result<BufferFrameIdT, String>;

    /// Indicate that the specified frame contains a pinned page and should
    /// not be evicted.
    /// Should be called after a page has been pinned to the buffer.
    fn pin(&mut self, frame_id: BufferFrameIdT);

    /// Indicate that the specified frame contains a page with a pin count
    /// of zero and can not be evicted.
    /// Should be called after a page reaches a pin count of zero.
    fn unpin(&mut self, frame_id: BufferFrameIdT);
}
