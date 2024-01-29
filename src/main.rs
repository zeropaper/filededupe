use std::env;
use std::fs::{self, DirEntry};
use std::fs::File;
use std::io::{self, BufReader, Read};
use md5;
use rusqlite::{params, Connection, Result};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        return Err("Usage: filededupe <directory_path>".into());
    }

    let directory = &args[1];

    let db_path = dirs::home_dir().unwrap().join(".filededupe.db");
    print!("Using database: {}", db_path.to_string_lossy());
    let conn = Connection::open(db_path)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS file_hashes (
             id INTEGER PRIMARY KEY,
             path TEXT NOT NULL UNIQUE,
             hash TEXT NOT NULL
         )",
        [],
    )?;

    if let Err(e) = process_directory(&conn, directory) {
        println!("Error: {}", e);
    }

    Ok(())
}

fn upsert_file_hash(conn: &Connection, path: &str, hash: &str) -> Result<(), rusqlite::Error> {
    conn.execute(
        "INSERT INTO file_hashes (path, hash) VALUES (?1, ?2)
         ON CONFLICT(path) DO UPDATE SET hash = excluded.hash",
        params![path, hash],
    )?;
    Ok(())
}

fn process_directory(conn: &Connection, path: &str) -> Result<(), std::io::Error> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            process_directory(conn, &path.to_string_lossy())?;
        } else {
            let (path_str, hash_str) = process_file(&entry)?;

            let query = conn.query_row(
                "SELECT hash FROM file_hashes WHERE path = ?1",
                params![path_str],
                |row| row.get::<_, String>(0),
            );

            match query {
                Ok(existing_hash) => {
                    println!("Skipped {}: {}", path_str, existing_hash);
                }
                Err(_) => {
                    upsert_file_hash(conn, &path_str, &hash_str)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                    println!("Processed {}: {}", path_str, hash_str);
                }
            }
        }
    }
    Ok(())
}

fn process_file(entry: &DirEntry) -> Result<(String, String), io::Error> {
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
    let hash_string = format!("{:x}", hash);

    Ok((path.to_string_lossy().into_owned(), hash_string))
}