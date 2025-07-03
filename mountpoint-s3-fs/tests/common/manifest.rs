use mountpoint_s3_fs::manifest::{create_db, ChannelManifest, DbEntry, ManifestError};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub fn create_manifest<I: Iterator<Item = Result<DbEntry, ManifestError>>>(
    channel_manifests: Vec<ChannelManifest<I>>,
    batch_size: usize,
) -> Result<(TempDir, PathBuf), ManifestError> {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("s3_keys.db3");

    create_db(&db_path, channel_manifests, batch_size)?;

    Ok((db_dir, db_path))
}

pub fn insert_entries(manifest_db_path: &Path, entries: &[DbEntry]) -> rusqlite::Result<()> {
    let mut conn = Connection::open(manifest_db_path).expect("must connect to a db");
    let tx = conn.transaction()?;
    let mut stmt = tx.prepare(
        "INSERT INTO s3_objects (id, key, name_offset, parent_id, etag, size, channel_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    for entry in entries {
        stmt.execute((
            entry.id,
            &entry.full_key,
            &entry.name_offset,
            entry.parent_id,
            entry.etag.as_deref(),
            entry.size,
            entry.channel_id,
        ))?;
    }
    drop(stmt);
    tx.commit()
}
