use std::env;
use std::fs::{self, DirEntry};
use std::fs::File;
use std::io::{self, BufReader, Read};
use md5;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Usage: filededupe <directory_path>");
        return;
    }

    let directory = &args[1];

    if let Err(e) = process_directory(directory) {
        println!("Error: {}", e);
    }
}


fn process_directory(path: &str) -> Result<(), std::io::Error> {
    // Read the directory and process each file
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            process_directory(&path.to_string_lossy())?;
        } else {
            process_file(&entry)?;
        }
    }
    Ok(())
}


fn process_file(entry: &DirEntry) -> Result<(), io::Error> {
    let path = entry.path();
    let file = File::open(&path)?;
    let mut reader = BufReader::new(file);
    let mut context = md5::Context::new();

    let mut buffer = [0; 8192]; // 8 KB buffer
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.consume(&buffer[..count]);
    }

    let hash = context.compute();
    println!("Processing file: {}", path.display());
    println!("MD5 hash: {:x}", hash);

    Ok(())
}