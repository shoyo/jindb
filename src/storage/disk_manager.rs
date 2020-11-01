use super::block::Block;
use super::constants::{BLOCK_SIZE, DB_FILENAME};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;

/// The disk manager is responsible for managing blocks stored on disk.

pub struct DiskManager {
    next_block_id: u32,
}

impl DiskManager {
    /// Create a new disk manager.
    pub fn new() -> Self {
        Self { next_block_id: 0 }
    }

    /// Write the specified byte array out to disk.
    pub fn write_block(
        &self,
        block_id: u32,
        block_data: &[u8; BLOCK_SIZE as usize],
    ) -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(DB_FILENAME)?;

        let offset = block_id * BLOCK_SIZE;
        file.seek(SeekFrom::Start(offset as u64))?;
        file.write_all(block_data)?;
        file.flush()?;

        Ok(())
    }

    /// Read a single block's data into the specified byte array.
    pub fn read_block(
        &self,
        block_id: u32,
        block_data: &mut [u8; BLOCK_SIZE as usize],
    ) -> std::io::Result<()> {
        let mut file = File::open(DB_FILENAME)?;

        let offset = block_id * BLOCK_SIZE;
        file.seek(SeekFrom::Start(offset as u64))?;
        file.read_exact(&mut *block_data)?;

        Ok(())
    }

    /// Allocate a block on disk and return the id of the allocated block.
    pub fn allocate_block(&mut self) -> u32 {
        let block_id = self.next_block_id;
        self.next_block_id += 1;
        block_id
    }

    /// Deallocate the specified block on disk.
    pub fn deallocate_block(&mut self, _block_id: u32) -> Result<(), ()> {
        Ok(())
    }
}
