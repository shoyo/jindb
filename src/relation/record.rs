/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use crate::common::{PageIdT, RecordSlotIdT};

use crate::relation::schema::Schema;
use crate::relation::types::Value;

/// A database record with variable-length attributes.
///
/// The initial section of the record contains a null bitmap which represents
/// which attributes are null and should be ignored.
///
/// The next section of a record contains fixed-length values. Data types
/// such as numerics, booleans, and dates are encoded as is, while variable-
/// length data types such as varchar are encoded as a offset/length pair.
///
/// The actual variable-length data is stored consecutively after the initial
/// fixed-length section and null bitmap.
///
/// Data format:
/// +-------------+---------------------+------------------------+
/// | NULL BITMAP | FIXED-LENGTH VALUES | VARIABLE-LENGTH VALUES |
/// +-------------+---------------------+------------------------+
///
/// Metadata regarding a record is stored in a system catalog in a separate
/// database page.

pub struct Record {
    id: RecordId,
    data: Vec<u8>,
}

impl Record {
    pub fn new(
        page_id: PageIdT,
        slot_index: u32,
        values: Vec<Box<dyn Value>>,
        schema: &Schema,
    ) -> Self {
        Self {
            id: RecordId {
                page_id,
                slot_index,
            },
            data: Vec::new(),
        }
    }

    pub fn get_id(&self) -> &RecordId {
        &self.id
    }

    pub fn get_data(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn get_column_value(&self, _idx: u32, _schema: &Schema) -> &[u8] {
        todo!()
    }
}

/// A database record identifier comprised of the page ID and slot index that
/// the record is located at.
pub struct RecordId {
    pub page_id: PageIdT,
    pub slot_index: RecordSlotIdT,
}
