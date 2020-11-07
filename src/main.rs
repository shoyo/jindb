use jin::*;

fn main() {
    println!("Jin (2020)");
    println!("Enter .help for usage hints");

    let dm = disk::manager::DiskManager::new();
    let bm = buffer::manager::BufferManager::new(128, dm);
}
