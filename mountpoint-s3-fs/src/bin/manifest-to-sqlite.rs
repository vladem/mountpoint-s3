use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use rusqlite::{Connection, Error};

use std::time::Instant;

use clap::Parser;

use base64ct::{Base64, Encoding};

#[derive(Parser, Debug)]
#[clap(name = "manifest-to-sqlite", about = "Helper for Mountpoint for Amazon S3")]
struct CliArgs {
    #[clap(
        help = "A CSV file of triplets <base64(name),etag,size> representing keys in the bucket",
        value_name = "INPUT_CSV"
    )]
    input_csv: String,

    #[clap(
        help = "Path to the output database file (will be created)",
        value_name = "OUTPUT_FILE"
    )]
    db_path: String,
}

#[derive(Debug)]
struct S3Object {
    key: String,
    parent_key: String,
    etag: String,
    size: u64,
}

impl S3Object {
    fn parse(raw: &str) -> Self {
        let tokens: Vec<&str> = raw.trim().split(",").collect();
        let decoded_key = Base64::decode_vec(tokens[0]).expect("must be a valid base64");
        let decoded_key = String::from_utf8(decoded_key).expect("must be a valid utf-8");
        assert!(!decoded_key.is_empty());
        let components: Vec<&str> = decoded_key.split("/").collect();
        let mut parent_key = components[..components.len() - 1].join("/");
        parent_key.push('/');
        Self {
            key: decoded_key,
            parent_key,
            etag: tokens[1].to_owned(),
            size: tokens[2].parse::<u64>().expect("must be a number"),
        }
    }
}

fn create_table_with_id(conn: &Connection) -> Result<(), Error> {
    conn.execute(
        "CREATE TABLE s3_objects (
            id          INTEGER   PRIMARY KEY,
            key         TEXT      NOT NULL,
            parent_key  TEXT      NOT NULL,
            etag        TEXT      NULL,
            size        INTEGER   NULL
        )",
        (),
    )?;

    Ok(())
}

fn create_index(conn: &Connection) -> Result<(), Error> {
    conn.execute("CREATE UNIQUE INDEX idx_key ON s3_objects (key)", ())?;

    conn.execute("CREATE INDEX idx_parent_key ON s3_objects (parent_key, key)", ())?;

    Ok(())
}

fn insert_from_file(conn: &Connection, path: &str, batch_size: usize) -> Result<usize, Error> {
    let mode: String = conn.query_row("PRAGMA journal_mode=off", [], |row| row.get(0))?;
    assert_eq!(&mode, "off");

    let mut counter = 0usize;

    let file = File::open(path).expect("input file must exist");
    let reader = BufReader::new(file);

    let mut buffer = Vec::<S3Object>::with_capacity(batch_size);
    for line in reader.lines() {
        counter += 1;

        let object = S3Object::parse(&line.expect("must be a line"));
        buffer.push(object);

        if buffer.len() >= batch_size {
            insert_batch(conn, &buffer)?;
            buffer.clear();
        }
    }

    Ok(counter)
}

fn insert_directories(conn: &Connection) -> Result<usize, Error> {
    let query = "SELECT key FROM s3_objects ORDER BY key";
    let mut stmt = conn.prepare(query)?;
    let keys_iter = stmt.query_map((), |row| {
        let key: String = row.get(0)?;
        Ok(key)
    })?;

    let mut prev_s3_key: Option<String> = None;
    let mut insert_buffer: Vec<S3Object> = Default::default();
    for s3_key in keys_iter {
        let s3_key = s3_key.expect("db field must be a valid string");
        let prev_components: Vec<&str> = if let Some(prev_s3_key) = &prev_s3_key {
            prev_s3_key.split("/").collect()
        } else {
            Default::default()
        };
        let components: Vec<&str> = s3_key.split("/").collect();

        // find the first subdirectory which wasn't created yet
        let mut longest_common_path_len = 0;
        let mut common_components_count = 0;
        for (idx, component) in components.iter().take(components.len() - 1).enumerate() {
            if idx >= prev_components.len() || *component != prev_components[idx] {
                break;
            }
            longest_common_path_len += component.len() + 1;
            common_components_count += 1;
        }

        // create new subdirectories
        let mut dir_key_len = longest_common_path_len;
        for component in components
            .iter()
            .take(components.len() - 1)
            .skip(common_components_count)
        {
            let parent_key_len = dir_key_len;
            dir_key_len += component.len() + 1; // includes the trailing '/'
            insert_buffer.push(S3Object {
                key: s3_key[..dir_key_len].to_owned(),
                parent_key: s3_key[..parent_key_len].to_owned(),
                etag: "".to_owned(),
                size: 0,
            });
        }

        prev_s3_key = Some(s3_key);
    }

    insert_batch(conn, &insert_buffer)?;

    Ok(insert_buffer.len())
}

fn insert_batch(conn: &Connection, objects: &[S3Object]) -> Result<(), Error> {
    conn.execute_batch("BEGIN TRANSACTION;")?;
    let mut stmt = conn.prepare("INSERT INTO s3_objects (key, parent_key, etag, size) VALUES (?1, ?2, ?3, ?4)")?;
    for object in objects {
        stmt.execute((&object.key, &object.parent_key, &object.etag, &object.size))?;
    }
    conn.execute_batch("COMMIT;")?;
    Ok(())
}

#[allow(unused)]
fn lookup(conn: &Connection) -> Result<(usize, usize), Error> {
    let query = "SELECT key, etag, size FROM s3_objects where key = ?1 LIMIT 1";
    let mut stmt = conn.prepare(query)?;
    let objects_iter = stmt.query_map(("12/sample-norABSnJCDdSUx6tZ2qtoK.jpg",), |row| {
        Ok(S3Object {
            key: row.get(0)?,
            parent_key: "".to_owned(),
            etag: row.get(1)?,
            size: row.get(2)?,
        })
    })?;

    let mut count = 0;
    let mut total_key_size = 0;
    for object in objects_iter {
        count += 1;
        let object = object.expect("must be a valid object");
        total_key_size += object.key.len();
    }

    Ok((count, total_key_size))
}

#[allow(unused)]
fn list(conn: &Connection, parent_key: &str) -> Result<(usize, usize), Error> {
    fn list_batch(
        conn: &Connection,
        next_offset: usize,
        batch_size: usize,
        parent_key: &str,
    ) -> Result<Vec<S3Object>, Error> {
        let query = "SELECT key, etag, size FROM s3_objects where parent_key = ?1 ORDER BY key LIMIT ?2, ?3";
        let mut stmt = conn.prepare(query)?;
        let objects_iter = stmt.query_map((parent_key, next_offset, batch_size), |row| {
            Ok(S3Object {
                key: row.get(0)?,
                parent_key: "".to_owned(),
                etag: row.get(1)?,
                size: row.get(2)?,
            })
        })?;

        let mut results = Vec::default();
        for object in objects_iter {
            let object = object.expect("must be a valid object");
            results.push(object);
        }

        Ok(results)
    }

    let batch_size = 10000;
    let mut next_offset = 0;
    let mut total_key_size = 0;

    loop {
        let batch = list_batch(conn, next_offset, batch_size, parent_key)?;
        for object in batch.iter() {
            next_offset += 1;
            total_key_size += object.key.len();
        }
        if batch.len() < batch_size {
            break;
        }
    }

    Ok((next_offset, total_key_size))
}

fn main() -> Result<(), Error> {
    let args = CliArgs::parse();

    let batch_size = 100000usize;

    let conn = Connection::open(&args.db_path)?;

    create_table_with_id(&conn)?;

    let start = Instant::now();
    let total = insert_from_file(&conn, &args.input_csv, batch_size)?;
    println!("insert {} objects in {:?}", total, start.elapsed());

    let start = Instant::now();
    let total = insert_directories(&conn)?;
    println!("insert {} dirs in {:?}", total, start.elapsed());

    let start = Instant::now();
    create_index(&conn)?;
    println!("created index in {:?}", start.elapsed());

    // let start = Instant::now();
    // let (count, total_key_size) = list(&conn, "12/")?;
    // println!("list 12/: queried {} objects in {:?}, total key size: {}", count, start.elapsed(), total_key_size);

    // let start = Instant::now();
    // let (count, total_key_size) = list(&conn, "")?;
    // println!("list root: queried {} objects in {:?}, total key size: {}", count, start.elapsed(), total_key_size);

    // let start = Instant::now();
    // let (count, total_key_size) = lookup(&conn)?;
    // println!("lookup: queried {} objects in {:?}, total key size: {}", count, start.elapsed(), total_key_size);

    Ok(())
}
