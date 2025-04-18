use rusqlite::{Connection, Result, Row};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

#[derive(Debug)]
pub struct DbEntry {
    pub full_key: String,
    pub etag: Option<String>,
    pub size: Option<usize>,
}

impl DbEntry {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Self {
            full_key: row.get(0)?,
            etag: row.get(1)?,
            size: row.get(2)?,
        })
    }
    /*
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
    */
    // Parent key without a trailing '/'
    fn parent_key(&self) -> &str {
        let key = self.full_key.trim_end_matches("/");
        let last_component_len = key.rsplit("/").next().expect("expect at least one component").len();
        if last_component_len == key.len() {
            // root case is special, it doesn't contain trailing '/'
            ""
        } else {
            &key[..key.len() - last_component_len - 1]
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    pub(super) fn new(manifest_db_path: &Path) -> Result<Self> {
        let conn = Connection::open(manifest_db_path)?;
        let mode: String = conn.query_row("PRAGMA journal_mode=off", [], |row| row.get(0))?;
        assert_eq!(&mode, "off");

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)), // TODO: connection pool?
        })
    }

    /// Queries rows from the DB representing either the file or a directory (typically a single row returned)
    pub(super) fn select_entries(&self, key: &str) -> Result<Vec<DbEntry>> {
        let start = Instant::now();
        let conn = self.conn.lock().expect("lock must succeed");
        metrics::histogram!("manifest.lookup.lock.elapsed_micros").record(start.elapsed().as_micros() as f64);

        let start = Instant::now();
        let query = "SELECT key, etag, size FROM s3_objects WHERE key = ?1";
        let mut stmt = conn.prepare(query)?;
        let result: Result<Vec<DbEntry>> = stmt.query_map((key,), DbEntry::from_row)?.collect();
        metrics::histogram!("manifest.lookup.query.elapsed_micros").record(start.elapsed().as_micros() as f64);

        result
    }

    /// Queries up to `batch_size` direct children of the directory with key `parent`, starting from `next_offset`
    pub(super) fn select_children(&self, parent: &str, next_offset: usize, batch_size: usize) -> Result<Vec<DbEntry>> {
        let start = Instant::now();
        let conn = self.conn.lock().expect("lock must succeed");
        metrics::histogram!("manifest.readdir.lock.elapsed_micros").record(start.elapsed().as_micros() as f64);

        let start = Instant::now();
        let query = "SELECT key, etag, size FROM s3_objects WHERE parent_key = ?1 ORDER BY key LIMIT ?2, ?3";
        let mut stmt = conn.prepare(query)?;
        let result: Result<Vec<DbEntry>> = stmt
            .query_map((parent, next_offset, batch_size), DbEntry::from_row)?
            .collect();
        metrics::histogram!("manifest.readdir.query.elapsed_micros").record(start.elapsed().as_micros() as f64);

        result
    }

    pub(super) fn create_table(&self) -> Result<()> {
        let conn = self.conn.lock().expect("lock must succeed");
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

    pub(super) fn create_index(&self) -> Result<()> {
        let conn = self.conn.lock().expect("lock must succeed");
        conn.execute("CREATE UNIQUE INDEX idx_key ON s3_objects (key)", ())?;

        conn.execute("CREATE INDEX idx_parent_key ON s3_objects (parent_key, key)", ())?;

        Ok(())
    }

    pub(super) fn insert_batch(&self, entries: &[DbEntry]) -> Result<()> {
        let conn = self.conn.lock().expect("lock must succeed");
        self.insert_batch_locked(&conn, entries)
    }

    /// Iterates over a sorted list of S3 object keys, infers and inserts the directories,
    /// deduplicating those based on the previous row.
    pub(super) fn insert_directories(&self, batch_size: usize) -> Result<usize> {
        let conn = self.conn.lock().expect("lock must succeed");
        let query = "SELECT key FROM s3_objects ORDER BY key";
        let mut stmt = conn.prepare(query)?;
        let keys_iter = stmt.query_map((), |row| {
            let key: String = row.get(0)?;
            Ok(key)
        })?;

        let mut prev_s3_key: Option<String> = None;
        let mut insert_buffer: Vec<DbEntry> = Default::default();
        let mut total = 0;
        for s3_key in keys_iter {
            let s3_key = s3_key?;
            insert_buffer.extend(infer_directories(prev_s3_key.as_deref(), &s3_key));
            prev_s3_key = Some(s3_key);

            if insert_buffer.len() >= batch_size {
                self.insert_batch_locked(&conn, &insert_buffer)?;
                total += insert_buffer.len();
                insert_buffer.clear();
            }
        }

        if !insert_buffer.is_empty() {
            self.insert_batch_locked(&conn, &insert_buffer)?;
            total += insert_buffer.len();
        }

        Ok(total)
    }

    fn insert_batch_locked(&self, conn: &MutexGuard<'_, Connection>, entries: &[DbEntry]) -> Result<()> {
        conn.execute_batch("BEGIN TRANSACTION;")?;
        let mut stmt = conn.prepare("INSERT INTO s3_objects (key, parent_key, etag, size) VALUES (?1, ?2, ?3, ?4)")?;
        for entry in entries {
            stmt.execute((&entry.full_key, entry.parent_key(), entry.etag.as_deref(), entry.size))?;
        }
        conn.execute_batch("COMMIT;")?;
        Ok(())
    }
}

fn infer_directories(prev_s3_key: Option<&str>, s3_key: &str) -> Vec<DbEntry> {
    let mut insert_buffer: Vec<DbEntry> = Default::default();
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
        let directory_key = &s3_key[..dir_key_len - 1];
        debug_assert!(!directory_key.ends_with("/"));
        insert_buffer.push(DbEntry {
            full_key: directory_key.to_owned(),
            etag: None,
            size: None,
        });
    }

    insert_buffer
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_entry_parent_key() {
        let entry = DbEntry {
            full_key: "a.txt".to_string(),
            etag: None,
            size: None,
        };
        assert_eq!(entry.parent_key(), "");

        let entry = DbEntry {
            full_key: "dir1/a.txt".to_string(),
            etag: None,
            size: None,
        };
        assert_eq!(entry.parent_key(), "dir1");

        let entry = DbEntry {
            full_key: "dir1/dir2/a.txt".to_string(),
            etag: None,
            size: None,
        };
        assert_eq!(entry.parent_key(), "dir1/dir2");

        let entry = DbEntry {
            full_key: "dir1".to_string(),
            etag: None,
            size: None,
        };
        assert_eq!(entry.parent_key(), "");

        let entry = DbEntry {
            full_key: "dir1/dir2".to_string(),
            etag: None,
            size: None,
        };
        assert_eq!(entry.parent_key(), "dir1");
    }
}
