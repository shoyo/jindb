use jin::buffer::manager::BufferManager;
use jin::disk::manager::DiskManager;
use jin::execution::system_catalog::SystemCatalog;
use jin::relation::schema::{Attribute, DataType, Schema};

fn main() {
    println!("Jin (2020)");
    println!("Enter .help for usage hints");

    let dm = DiskManager::new();
    let bm = BufferManager::new(dm);
    let mut catalog = SystemCatalog::new(bm);

    let schema = Schema::new(vec![
        Attribute {
            name: "full_name".to_string(),
            data_type: DataType::Varchar,
            nullable: false,
        },
        Attribute {
            name: "age".to_string(),
            data_type: DataType::TinyInt,
            nullable: false,
        },
        Attribute {
            name: "school".to_string(),
            data_type: DataType::Varchar,
            nullable: false,
        },
    ]);
    catalog.create_relation("Students".to_string(), schema);
}
