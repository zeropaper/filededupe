use std::env;
use std::fs::{self};
use std::fs::File;
use std::io::{self, BufReader, Read};
use md5;
use rusqlite::{params, Connection, Result};

const BATCH_SIZE: usize = 20;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        return Err("Usage: filededupe <directory_path>".into());
    }

    let directory = &args[1];

    let db_path = dirs::home_dir().unwrap().join(".filededupe.db");
    println!("Using database: {}", db_path.to_string_lossy());
    let conn = Connection::open(db_path)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS file_hashes (
             id INTEGER PRIMARY KEY,
             path TEXT NOT NULL UNIQUE,
             hash TEXT NOT NULL,
             size INTEGER NOT NULL
         )",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_path ON file_hashes(path)",
        [],
    )?;

    if let Err(e) = process_directory(&conn, directory) {
        println!("Error: {}", e);
    }

    Ok(())
}

fn upsert_file_hash(conn: &Connection, path: &str, hash: &str, size: i64) -> Result<(), rusqlite::Error> {
    conn.execute(
        "INSERT INTO file_hashes (path, hash, size) VALUES (?1, ?2, ?3)
         ON CONFLICT(path) DO UPDATE SET hash = excluded.hash, size = excluded.size",
        params![path, hash, size],
    )?;
    Ok(())
}

fn process_directory(conn: &Connection, path: &str) -> Result<(), std::io::Error> {
    let mut file_data_batch = Vec::new();

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            println!("Processing directory: {}", path.to_string_lossy());
            process_directory(conn, &path.to_string_lossy())?;
        } else {
            println!("Processing file: {}", path.to_string_lossy());
            let metadata = entry.metadata()?;
            let file_data = (path.to_string_lossy().into_owned(), metadata.len() as i64);
            file_data_batch.push(file_data);

            if file_data_batch.len() >= BATCH_SIZE {
                process_files_batch(conn, &file_data_batch)?;
                file_data_batch.clear();
            }
        }
    }

    if !file_data_batch.is_empty() {
        println!("Processing remaining files");
        process_files_batch(conn, &file_data_batch)?;
    }

    Ok(())
}

fn compute_file_hash(path: &str) -> Result<String, io::Error> {
    let file = File::open(path)?;
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
    Ok(format!("{:x}", hash))
}

fn process_files_batch(conn: &Connection, batch: &[(String, i64)]) -> Result<(), std::io::Error> {
    for (path, size) in batch {
        let hash = compute_file_hash(path)?;
        upsert_file_hash(conn, path, &hash, *size)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    }

    Ok(())
}
