use super::ManifestError;
use crate::manifest::db::{Db, DbEntry};
use std::path::Path;

// TODO: directory shadowing ("a/b" key not available if "a/b/c.txt" exists)
// TODO: s3 keys ending with '/'?
pub fn create_db_from_csv(_db_path: &Path, _csv_path: &Path, _batch_size: usize) -> Result<(), ManifestError> {
    Ok(())
}

// todo: make this method accepting an iterator over ManifestEntry (=> ingestion tests without CSV / parquet / etc)
pub fn create_db_from_slice(db_path: &Path, entries: &[DbEntry]) -> Result<(), rusqlite::Error> {
    let db = Db::new(db_path)?;
    db.create_table()?;
    db.insert_batch(entries)?;
    db.insert_directories(1024)?;
    db.create_index()?;
    Ok(())
}
