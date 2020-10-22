use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;

const PAGE_SIZE: usize = 32;
const DB_FILENAME: &str = "db.minusql";

fn main() {
    println!("minuSQL (2020)");
    println!("Enter .help for usage hints");
    //    loop {
    //        print!("minuSQL > ");
    //        io::stdout().flush().unwrap();
    //
    //        let mut query = String::new();
    //        io::stdin()
    //            .read_line(&mut query)
    //            .expect("Error reading input");
    //        println!("TODO");
    //    }

    write_to_disk(0, "hello world!".to_string());
    write_to_disk(1, "foobar".to_string());
    write_to_disk(2, "a".to_string());

    let data = read_from_disk(0).unwrap();
    println!("Read: {}", std::str::from_utf8(&data).unwrap());
    let data = read_from_disk(1).unwrap();
    println!("Read: {}", std::str::from_utf8(&data).unwrap());
    let data = read_from_disk(2).unwrap();
    println!("Read: {}", std::str::from_utf8(&data).unwrap());

    write_to_disk(1, "changed".to_string());
    let data = "a".to_string();
    let data = read_from_disk(1).unwrap();
    println!("Read: {}", std::str::from_utf8(&data).unwrap());
}

fn write_to_disk(page_id: usize, data: String) -> std::io::Result<()> {
    if data.len() > PAGE_SIZE {
        eprintln!(
            "Error: Attempted to write data that exceeds page size of {}B",
            PAGE_SIZE
        );
        std::process::exit(1);
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(DB_FILENAME)?;
    let offset = page_id * PAGE_SIZE;
    file.seek(SeekFrom::Start(offset.try_into().unwrap()))?;

    file.write_all(data.as_bytes());
    for _ in 0..(PAGE_SIZE - data.len()) {
        file.write(&[0]);
    }

    Ok(())
}

fn read_from_disk(page_id: usize) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(DB_FILENAME)?;
    let offset = page_id * PAGE_SIZE;
    file.seek(SeekFrom::Start(offset.try_into().unwrap()))?;

    let mut buf = [0; PAGE_SIZE];
    file.read_exact(&mut buf)?;

    Ok(buf.to_vec())
}
