use minusql::{buffer, storage};

fn main() {
    println!("minuSQL (2020)");
    println!("Enter .help for usage hints");

    let dm = storage::disk_manager::DiskManager::new();
    let bm = buffer::buffer_manager::BufferManager::new(128, dm);
}
