use super::{ManifestError, ManifestWarning};
use crate::manifest::db::{Db, DbEntry};
use std::path::Path;

pub fn create_db(
    db_path: &Path,
    entries: impl Iterator<Item = Result<DbEntry, ManifestError>>,
    batch_size: usize,
) -> Result<Vec<ManifestWarning>, ManifestError> {
    let db = Db::new(db_path)?;
    db.create_table()?;

    let mut buffer = Vec::with_capacity(batch_size);
    for entry in entries {
        buffer.push(entry?); // todo: stop on input error?

        if buffer.len() >= batch_size {
            db.insert_batch(&buffer)?;
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        db.insert_batch(&buffer)?;
    }

    db.create_index()?;

    let warnings = db
        .insert_directories(batch_size)?
        .into_iter()
        .map(|db_entry| ManifestWarning::ShadowedKey(db_entry.full_key))
        .collect();
    Ok(warnings)
}
