use crate::sync::{Arc, Mutex};
use std::time::Instant;
use std::{collections::VecDeque, path::Path};

use rusqlite::Connection;
use tracing::{error, trace};

use crate::superblock::{Inode, InodeError, InodeKind};

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
    fn from(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
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
        parent: Inode,
        parent_full_path: String,
        name: &str,
    ) -> Result<ManifestEntry, InodeError> {
        trace!("using manifest to lookup {} in {}", name, parent_full_path);

        if parent.kind() != InodeKind::Directory {
            return Err(InodeError::NotADirectory(parent.err()));
        }

        let mut full_path = parent_full_path;
        full_path.push_str(name);

        let mut dir_key = String::with_capacity(full_path.len() + 1);
        dir_key.push_str(&full_path);
        dir_key.push('/');

        // search for an entry
        let manifest_entry = self
            .db
            .select_entry(&full_path, &dir_key)
            .inspect_err(|err| error!("failed to query the database: {}", err))
            .map_err(|_| InodeError::InodeDoesNotExist(0))?; // TODO: ManifestError::DbError

        // return an inode or error
        match manifest_entry {
            Some(manifest_entry) => Ok(manifest_entry.clone()),
            None => Err(InodeError::FileDoesNotExist(name.to_owned(), parent.err())),
        }
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
    pub fn next(&mut self) -> Result<Option<ManifestEntry>, InodeError> {
        if self.entries.is_empty() && !self.finished {
            self.search_next_entries()
                .inspect_err(|err| error!("failed to query the database: {}", err))
                .map_err(|_| InodeError::InodeDoesNotExist(0))?; // TODO: ManifestError::DbError
        }

        Ok(self.entries.pop_front())
    }

    /// Load next batch of entries from the database, keeping track of the `next_offset`
    fn search_next_entries(&mut self) -> Result<(), rusqlite::Error> {
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
    fn select_entry(&self, key: &str, dir_key: &str) -> Result<Option<ManifestEntry>, rusqlite::Error> {
        let start = Instant::now();
        let conn = self.conn.lock().expect("lock must succeed");
        metrics::histogram!("manifest.lookup.lock.elapsed_micros").record(start.elapsed().as_micros() as f64);

        let start = Instant::now();
        let query = "SELECT key, etag, size FROM s3_objects WHERE key = ?1 OR key = ?2";
        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query_map((key, &dir_key), ManifestEntry::from)?;
        let manifest_entry = rows.next();
        debug_assert!(rows.next().is_none(), "query expected to return one row: {}", key); // TODO: return an error?
        metrics::histogram!("manifest.lookup.query.elapsed_micros").record(start.elapsed().as_micros() as f64);

        manifest_entry.map_or(Ok(None), |v| v.map(Some))
    }

    /// Queries up to `batch_size` direct children of the directory with key `parent`, starting from `next_offset`
    fn select_children(
        &self,
        parent: &str,
        next_offset: usize,
        batch_size: usize,
    ) -> Result<Vec<ManifestEntry>, rusqlite::Error> {
        let start = Instant::now();
        let conn = self.conn.lock().expect("lock must succeed");
        metrics::histogram!("manifest.readdir.lock.elapsed_micros").record(start.elapsed().as_micros() as f64);

        let start = Instant::now();
        let query = "SELECT key, etag, size FROM s3_objects WHERE parent_key = ?1 ORDER BY key LIMIT ?2, ?3";
        let mut stmt = conn.prepare(query)?;
        let result: Result<Vec<_>, _> = stmt
            .query_map((parent, next_offset, batch_size), ManifestEntry::from)?
            .collect();
        metrics::histogram!("manifest.readdir.query.elapsed_micros").record(start.elapsed().as_micros() as f64);

        result
    }
}
