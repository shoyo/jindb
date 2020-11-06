use jin::*;

fn main() {
    println!("Jin (2020)");
    println!("Enter .help for usage hints");

    let dm = disk::disk_manager::DiskManager::new();
    let bm = buffer::buffer_manager::BufferManager::new(128, dm);
}
