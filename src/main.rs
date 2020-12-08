use jin::buffer::manager::BufferManager;
use jin::disk::manager::DiskManager;
use jin::execution::system_catalog::SystemCatalog;
use jin::relation::attribute::{Attribute, DataType};
use jin::relation::record::Record;
use jin::relation::schema::Schema;

fn main() {
    println!("Jin (2020)");
    println!("Enter .help for usage hints");

    let buffer = BufferManager::new(DiskManager::new());
    let mut catalog = SystemCatalog::new(buffer);

    let guard = catalog
        .create_relation(
            "Students".to_string(),
            Schema::new(vec![
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
            ]),
        )
        .unwrap();

    {
        let relation = guard.lock().unwrap();
    }
}
