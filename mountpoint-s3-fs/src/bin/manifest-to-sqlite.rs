use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use rusqlite::{Connection, Error};

use std::time::Instant;

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(name = "manifest-to-sqlite", about = "Helper for Mountpoint for Amazon S3")]
struct CliArgs {
    #[clap(
        help = "A TSV file of triplets <name,etag,size> representing keys in the bucket",
        value_name = "INPUT_TSV"
    )]
    input_tsv: String,

    #[clap(
        help = "Path to the output database file (will be created)",
        value_name = "OUTPUT_FILE"
    )]
    db_path: String,
}

#[derive(Debug)]
struct S3Object {
    key: String,
    etag: String,
    size: u64,
}

impl S3Object {
    fn parse(raw: &str) -> Self {
        let tokens: Vec<&str> = raw.trim().split("\t").collect();
        Self {
            key: tokens[0].to_owned(),
            etag: tokens[1].to_owned(),
            size: tokens[2].parse::<u64>().expect("must be a number"),
        }
    }
}

fn create_table_with_id(conn: &Connection) -> Result<(), Error> {
    conn.execute(
        "CREATE TABLE s3_objects (
            id      INTEGER   PRIMARY KEY,
            key     TEXT      NOT NULL,
            etag    TEXT      NOT NULL,
            size    INTEGER   NOT NULL
        )",
        (),
    )?;

    Ok(())
}

fn create_table(conn: &Connection) -> Result<(), Error> {
    conn.execute(
        "CREATE TABLE s3_objects (
            key     TEXT      PRIMARY KEY,
            etag    TEXT      NOT NULL,
            size    INTEGER   NOT NULL
        )",
        (),
    )?;

    Ok(())
}

fn create_index(conn: &Connection) -> Result<(), Error> {
    conn.execute("CREATE INDEX idx_key ON s3_objects (key)", ())?;

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
        counter = counter + 1;

        let object = S3Object::parse(&line.expect("must be a line"));
        buffer.push(object);

        if buffer.len() >= batch_size {
            insert_batch(&conn, &buffer)?;
            buffer.clear();
        }
    }

    Ok(counter)
}

fn insert_batch(conn: &Connection, objects: &[S3Object]) -> Result<(), Error> {
    conn.execute_batch("BEGIN TRANSACTION;")?;
    let mut stmt = conn.prepare("INSERT INTO s3_objects (key, etag, size) VALUES (?1, ?2, ?3)")?;
    for object in objects {
        stmt.execute((&object.key, &object.etag, &object.size))?;
    }
    conn.execute_batch("COMMIT;")?;
    Ok(())
}

fn query_objects(conn: &Connection) -> Result<(usize, usize), Error> {
    // let query = "SELECT key, etag, size FROM s3_objects ORDER BY key";
    // let query = "SELECT key, etag, size FROM s3_objects where key LIKE ?1";
    let query = "SELECT key, etag, size FROM s3_objects where key >= ?1 and key < ?2 ORDER BY key LIMIT 10000";
    // let query = "SELECT key, etag, size FROM s3_objects where (key >= ?1 and key < ?2) or key = ?3 LIMIT 1";
    // let query = "SELECT key, etag, size FROM s3_objects where key = ?1";
    let mut stmt = conn.prepare(query)?;
    let objects_iter = stmt.query_map(
        // (),
        // ("44/%",),
        ("44/", ("440")),
        // ("44/sample-bQJuuySjsf9ThPeQHDxVTg.jpg",),
        // ("44/", "440", "44",),
        // ()
        |row| {
            Ok(S3Object {
                key: row.get(0)?,
                etag: row.get(1)?,
                size: row.get(2)?,
            })
        },
    )?;

    let mut count = 0;
    let mut total_key_size = 0;
    for object in objects_iter {
        count = count + 1;
        total_key_size = total_key_size + object.expect("must be a valid object").key.len();
    }

    Ok((count, total_key_size))
}

fn main() -> Result<(), Error> {
    let args = CliArgs::parse();

    let batch_size = 100000usize;

    let conn = Connection::open(&args.db_path)?;

    // create_table(&conn)?;
    create_table_with_id(&conn)?;

    let start = Instant::now();
    let total = insert_from_file(&conn, &args.input_tsv, batch_size)?;
    println!("insert {} objects in {:?}", total, start.elapsed());

    let start = Instant::now();
    create_index(&conn)?;
    println!("created index in {:?}", start.elapsed());

    // let start = Instant::now();
    // let (count, total_key_size) = query_objects(&conn)?;
    // println!("queried {} objects in {:?}, total key size: {}", count, start.elapsed(), total_key_size);

    Ok(())
}
