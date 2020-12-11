use crate::buffer::manager::BufferManager;
use crate::execution::system_catalog::SystemCatalog;
use crate::execution::transaction::Transaction;

pub struct ExecutorMeta<'a> {
    transaction: &'a Transaction,
    system_catalog: &'a SystemCatalog,
    buffer_manager: &'a BufferManager,
    // TODO: Implement and add log and lock managers
}

impl<'a> ExecutorMeta<'a> {
    pub fn new(
        transaction: &'a Transaction,
        system_catalog: &'a SystemCatalog,
        buffer_manager: &'a BufferManager,
    ) -> Self {
        Self {
            transaction,
            system_catalog,
            buffer_manager,
        }
    }
}
