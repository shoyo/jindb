/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::constants::{LsnT, TransactionIdT};
use std::collections::HashMap;

pub struct LogManager;

struct LogRecovery {
    log_buffer: String,

    /// Mapping of active transactions to latest LSN
    active: HashMap<TransactionIdT, LsnT>,

    /// Mapping of LSN to log file offset for undo operations
    lsn_offsets: HashMap<LsnT, i32>,
}

impl LogRecovery {
    pub fn redo() {
        todo!()
    }

    pub fn undo() {
        todo!()
    }
}
