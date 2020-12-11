use crate::block::table_block::TableBlock;
use crate::common::constants::{LsnT, TransactionIdT, INVALID_LSN};
use crate::relation::record::{Record, RecordId};
use crate::relation::relation::{Relation, RelationGuard};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

struct Transaction {
    /// Unique ID for this transaction
    id: TransactionIdT,

    /// State of transaction
    state: TransactionState,

    /// Log sequence number of last record written by this transaction
    prev_lsn: LsnT,

    /// Write records ordered chronologically for an undo
    write_record_q: Arc<Mutex<VecDeque<WriteRecord>>>,

    /// Blocks that were latched during index operation
    block_q: Arc<Mutex<VecDeque<TableBlock>>>,

    /// Blocks that were deleted during index operation
    delete_block_q: Arc<Mutex<VecDeque<TableBlock>>>,

    /// Shared-lock records held by this transaction
    shared_lock_q: Arc<Mutex<VecDeque<RecordId>>>,

    /// Exclusive-locked records held by this transaction
    exclusive_lock_q: Arc<Mutex<VecDeque<RecordId>>>,
}

impl Transaction {
    pub fn new(id: TransactionIdT) -> Self {
        Self {
            id,
            state: TransactionState::Growing,
            prev_lsn: INVALID_LSN,
            write_record_q: Arc::new(Mutex::new(VecDeque::new())),
            block_q: Arc::new(Mutex::new(VecDeque::new())),
            delete_block_q: Arc::new(Mutex::new(VecDeque::new())),
            shared_lock_q: Arc::new(Mutex::new(VecDeque::new())),
            exclusive_lock_q: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

enum TransactionState {
    Growing,
    Shrinking,
    Committed,
    Aborted,
}

enum WriteOp {
    Insert,
    Delete,
    Update,
}

struct WriteRecord {
    /// Record ID for the record to be written.
    rid: RecordId,

    /// Record to be written. Stores Some for an update operation, None otherwise.
    record: Option<Record>,

    /// Write operation to be performed
    op: WriteOp,

    /// Relation affected by this write operation
    relation: RelationGuard,
}
