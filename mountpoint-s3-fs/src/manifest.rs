use crate::sync::{Arc, Mutex};
use std::time::Instant;
use std::{collections::VecDeque, path::Path};

use rusqlite::Connection;
use thiserror::Error;
use tracing::{error, trace};

use crate::superblock::InodeError;

use base64ct::{Base64, Encoding};

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("database error")]
    DbError(#[from] rusqlite::Error),
    #[error("file shadowed by a directory must not exist in db")]
    UnexpectedShadowedFile(String),
    #[error("invalid csv")]
    InvalidCsv,
}

/// An entry returned by manifest_lookup() and ManifestIter::next()
#[derive(Debug, Clone)]
pub enum ManifestEntry {
    File {
        full_key: String,
        etag: String,
        size: usize,
    },
    Directory {
        full_key: String, // let's assume it always ends with '/'
    },
}

impl ManifestEntry {
    fn from(row: &rusqlite::Row) -> Result<Self, ManifestError> {
        let full_key: String = row.get(0)?;
        let entry = if full_key.ends_with('/') {
            Self::Directory { full_key }
        } else {
            Self::File {
                full_key,
                etag: row.get(1)?,
                size: row.get(2)?,
            }
        };
        Ok(entry)
    }

    fn from_csv(csv_row: &str) -> Result<Self, ManifestError> {
        let tokens: Vec<&str> = csv_row.trim().split(",").collect();
        let decoded_key = Base64::decode_vec(tokens[0]).map_err(|_| ManifestError::InvalidCsv)?;
        let decoded_key = String::from_utf8(decoded_key).map_err(|_| ManifestError::InvalidCsv)?;
        if decoded_key.is_empty() {
            Err(ManifestError::InvalidCsv)
        } else {
            Ok(Self::File {
                full_key: decoded_key,
                etag: tokens[1].to_owned(),
                size: tokens[2].parse::<usize>().map_err(|_| ManifestError::InvalidCsv)?,
            })
        }
    }

    fn key(&self) -> &str {
        match self {
            ManifestEntry::File { full_key, .. } => full_key,
            ManifestEntry::Directory { full_key } => full_key,
        }
    }

    fn parent_key(&self) -> &str {
        let key = self.key().trim_end_matches("/");
        let last_component_len = key.rsplit("/").next().expect("expect at least one component").len();
        if last_component_len == key.len() {
            // root parent is special, it doesn't contain '/'
            ""
        } else {
            &key[..key.len() - last_component_len]
        }
    }
}

/// Manifest of all available objects in the bucket
#[derive(Debug)]
pub struct Manifest {
    db: Db,
}

impl Manifest {
    pub fn new(manifest_db_path: &Path) -> Result<Self, rusqlite::Error> {
        let db = Db::new(manifest_db_path)?;
        Ok(Self { db })
    }

    /// Lookup an entry in the manifest, the result may be a file or a directory
    pub fn manifest_lookup(
        &self,
        parent_full_path: String,
        name: &str,
    ) -> Result<Option<ManifestEntry>, ManifestError> {
        trace!("using manifest to lookup {} in {}", name, parent_full_path);
        let mut full_path = parent_full_path;
        full_path.push_str(name);

        let mut dir_key = String::with_capacity(full_path.len() + 1);
        dir_key.push_str(&full_path);
        dir_key.push('/');

        // search for an entry
        self.db.select_entry(&full_path, &dir_key)
    }

    /// Create an iterator over directory's direct children
    pub fn iter(&self, bucket: &str, directory_full_path: &str) -> Result<ManifestIter, InodeError> {
        ManifestIter::new(self.db.clone(), bucket, directory_full_path)
    }
}

#[derive(Debug)]
pub struct ManifestIter {
    db: Db,
    /// Prepared entries in order to be returned by the iterator.
    entries: VecDeque<ManifestEntry>,
    /// Key of the directory being listed by this iterator
    parent_key: String,
    /// Offset of the next child to search for in the database
    next_offset: usize,
    /// Max amount of entries to read from the database at once
    batch_size: usize,
    /// Database has no more entries
    finished: bool,
}

impl ManifestIter {
    fn new(db: Db, _bucket: &str, parent_key: &str) -> Result<Self, InodeError> {
        let parent_key = parent_key.to_owned();
        let batch_size = 10000;
        Ok(Self {
            db,
            entries: Default::default(),
            parent_key,
            next_offset: 0,
            batch_size,
            finished: false,
        })
    }

    /// Next child of the directory
    pub fn next_entry(&mut self) -> Result<Option<ManifestEntry>, ManifestError> {
        if self.entries.is_empty() && !self.finished {
            self.search_next_entries()?
        }

        Ok(self.entries.pop_front())
    }

    /// Load next batch of entries from the database, keeping track of the `next_offset`
    fn search_next_entries(&mut self) -> Result<(), ManifestError> {
        let db_entries = self
            .db
            .select_children(&self.parent_key, self.next_offset, self.batch_size)?;

        if db_entries.len() < self.batch_size {
            self.finished = true;
        }

        self.next_offset += db_entries.len();
        self.entries.extend(db_entries);

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    fn new(manifest_db_path: &Path) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(manifest_db_path)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)), // TODO: no mutex? serialized mode of sqlite?
        })
    }

    /// Queries single row from the DB representing either the file or a directory
    fn select_entry(&self, key: &str, dir_key: &str) -> Result<Option<ManifestEntry>, ManifestError> {
        let start = Instant::now();
        let conn = self.conn.lock().expect("lock must succeed");
        metrics::histogram!("manifest.lookup.lock.elapsed_micros").record(start.elapsed().as_micros() as f64);

        let start = Instant::now();
        let query = "SELECT key, etag, size FROM s3_objects WHERE key = ?1 OR key = ?2";
        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query((key, &dir_key))?;
        let row = rows.next()?;
        let manifest_entry = match row {
            Some(row) => Some(ManifestEntry::from(row)?),
            None => None,
        };
        if rows.next()?.is_some() {
            // there is a UNIQUE index on key, so we found a file and directory with this query,
            // the file is expected to be filtered out on ingestion of csv to db
            return Err(ManifestError::UnexpectedShadowedFile(key.to_string()));
        }
        metrics::histogram!("manifest.lookup.query.elapsed_micros").record(start.elapsed().as_micros() as f64);

        Ok(manifest_entry)
    }

    /// Queries up to `batch_size` direct children of the directory with key `parent`, starting from `next_offset`
    fn select_children(
        &self,
        parent: &str,
        next_offset: usize,
        batch_size: usize,
    ) -> Result<Vec<ManifestEntry>, ManifestError> {
        let start = Instant::now();
        let conn = self.conn.lock().expect("lock must succeed");
        metrics::histogram!("manifest.readdir.lock.elapsed_micros").record(start.elapsed().as_micros() as f64);

        let start = Instant::now();
        let query = "SELECT key, etag, size FROM s3_objects WHERE parent_key = ?1 ORDER BY key LIMIT ?2, ?3";
        let mut stmt = conn.prepare(query)?;
        let mut result = Vec::with_capacity(batch_size);
        let mut rows = stmt.query((parent, next_offset, batch_size))?;
        while let Some(row) = rows.next()? {
            result.push(ManifestEntry::from(row)?)
        }
        metrics::histogram!("manifest.readdir.query.elapsed_micros").record(start.elapsed().as_micros() as f64);

        Ok(result)
    }
}

pub mod builder {
    use super::{ManifestEntry, ManifestError};
    use rusqlite::Connection;
    use std::{
        fs::File,
        io::{BufRead, BufReader},
        path::Path,
    };

    // Creates a db from ManifestEntry representing keys of S3 objects. Infers parent directories.
    // Used in tests, not optimized for large datasets.
    #[cfg(feature = "manifest_tests")]
    pub fn create_db_from_slice(db_path: &Path, entries: &[ManifestEntry]) -> Result<(), rusqlite::Error> {
        let db = DbWriter::new(db_path)?;
        db.create_table_with_id()?;
        db.insert_batch(entries)?;
        db.insert_directories()?;
        db.create_index()?;
        Ok(())
    }

    #[cfg(feature = "manifest_tests")]
    pub fn insert_row(db_path: &Path, row: (&str, &str, Option<&str>, Option<usize>)) -> Result<(), rusqlite::Error> {
        let db = DbWriter::new(db_path)?;
        db.insert_row(row)
    }

    // TODO: directory shadowing ("a/b" key not available if "a/b/c.txt" exists)
    // TODO: s3 keys ending with '/'?
    pub fn create_db_from_csv(db_path: &Path, csv_path: &Path, batch_size: usize) -> Result<(), ManifestError> {
        let db = DbWriter::new(db_path)?;
        db.create_table_with_id()?;

        let file = File::open(csv_path).expect("input file must exist");
        let reader = BufReader::new(file);
        let mut buffer = Vec::<ManifestEntry>::with_capacity(batch_size);
        for line in reader.lines() {
            let entry = ManifestEntry::from_csv(&line.map_err(|_| ManifestError::InvalidCsv)?)?;
            buffer.push(entry);

            if buffer.len() >= batch_size {
                db.insert_batch(&buffer)?;
                buffer.clear();
            }
        }

        db.insert_directories()?;
        db.create_index()?;
        Ok(())
    }

    struct DbWriter {
        conn: Connection,
    }

    impl DbWriter {
        fn new(manifest_db_path: &Path) -> Result<Self, rusqlite::Error> {
            let conn = Connection::open(manifest_db_path)?;
            let mode: String = conn.query_row("PRAGMA journal_mode=off", [], |row| row.get(0))?;
            assert_eq!(&mode, "off");

            Ok(Self { conn })
        }

        fn create_table_with_id(&self) -> Result<(), rusqlite::Error> {
            self.conn.execute(
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

        fn create_index(&self) -> Result<(), rusqlite::Error> {
            self.conn
                .execute("CREATE UNIQUE INDEX idx_key ON s3_objects (key)", ())?;

            self.conn
                .execute("CREATE INDEX idx_parent_key ON s3_objects (parent_key, key)", ())?;

            Ok(())
        }

        fn insert_batch(&self, entries: &[ManifestEntry]) -> Result<(), rusqlite::Error> {
            self.conn.execute_batch("BEGIN TRANSACTION;")?;
            let mut object_key_stmt = self
                .conn
                .prepare("INSERT INTO s3_objects (key, parent_key, etag, size) VALUES (?1, ?2, ?3, ?4)")?;
            let mut directory_key_stmt = self
                .conn
                .prepare("INSERT INTO s3_objects (key, parent_key) VALUES (?1, ?2)")?;
            for entry in entries {
                match entry {
                    ManifestEntry::File { full_key, etag, size } => {
                        object_key_stmt.execute((full_key, entry.parent_key(), etag, size))?;
                    }
                    ManifestEntry::Directory { full_key } => {
                        directory_key_stmt.execute((full_key, entry.parent_key()))?;
                    }
                }
            }
            self.conn.execute_batch("COMMIT;")?;
            Ok(())
        }

        // TODO: insert directories in batches
        fn insert_directories(&self) -> Result<usize, rusqlite::Error> {
            let query = "SELECT key FROM s3_objects ORDER BY key";
            let mut stmt = self.conn.prepare(query)?;
            let keys_iter = stmt.query_map((), |row| {
                let key: String = row.get(0)?;
                Ok(key)
            })?;

            let mut prev_s3_key: Option<String> = None;
            let mut insert_buffer: Vec<ManifestEntry> = Default::default();
            for s3_key in keys_iter {
                let s3_key = s3_key?;
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
                    dir_key_len += component.len() + 1; // includes the trailing '/'
                    let directory_key = &s3_key[..dir_key_len];
                    debug_assert!(directory_key.ends_with("/"));
                    insert_buffer.push(ManifestEntry::Directory {
                        full_key: directory_key.to_owned(),
                    });
                }

                prev_s3_key = Some(s3_key);
            }

            self.insert_batch(&insert_buffer)?;

            Ok(insert_buffer.len())
        }

        #[cfg(feature = "manifest_tests")]
        fn insert_row(&self, row: (&str, &str, Option<&str>, Option<usize>)) -> Result<(), rusqlite::Error> {
            let mut object_key_stmt = self
                .conn
                .prepare("INSERT INTO s3_objects (key, parent_key, etag, size) VALUES (?1, ?2, ?3, ?4)")?;
            object_key_stmt.execute(row)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_entry_parent_key() {
        let entry = ManifestEntry::File {
            full_key: "a.txt".to_string(),
            etag: "".to_string(),
            size: 0,
        };
        assert_eq!(entry.parent_key(), "");

        let entry = ManifestEntry::File {
            full_key: "dir1/a.txt".to_string(),
            etag: "".to_string(),
            size: 0,
        };
        assert_eq!(entry.parent_key(), "dir1/");

        let entry = ManifestEntry::File {
            full_key: "dir1/dir2/a.txt".to_string(),
            etag: "".to_string(),
            size: 0,
        };
        assert_eq!(entry.parent_key(), "dir1/dir2/");

        let entry = ManifestEntry::Directory {
            full_key: "dir1/".to_string(),
        };
        assert_eq!(entry.parent_key(), "");

        let entry = ManifestEntry::Directory {
            full_key: "dir1/dir2/".to_string(),
        };
        assert_eq!(entry.parent_key(), "dir1/");
    }
}
