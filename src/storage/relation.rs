pub struct Schema {
    columns: Vec<Column>,
}

pub struct Column {
    name: String,
    data_type: DataType,
}

pub enum DataType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Decimal,
    Varchar,
}

pub struct Relation {
    name: String,
    schema: Schema,
}
