use std::io::{BufRead, Lines};
use std::{fs::File, io::BufReader, path::Path};

use super::{DbEntry, ManifestError};

pub struct CsvReader {
    reader: Lines<BufReader<File>>,
}

impl CsvReader {
    pub fn new(csv_path: &Path) -> Result<Self, ManifestError> {
        let file = File::open(csv_path).expect("input file must exist");
        let reader = BufReader::new(file).lines(); // TODO: \n? \r\n?
        Ok(Self { reader })
    }
}

impl Iterator for CsvReader {
    type Item = Result<DbEntry, ManifestError>;

    fn next(&mut self) -> Option<Self::Item> {
        let line = self.reader.next()?;
        let Ok(line) = line else {
            return Some(Err(ManifestError::InputError));
        };
        Some(db_entry_from_csv(&line))
    }
}

fn db_entry_from_csv(csv_row: &str) -> Result<DbEntry, ManifestError> {
    let tokens: Vec<&str> = csv_row.trim().split(",").collect();
    let raw_key = tokens[0];
    if raw_key.len() < 2 {
        return Err(ManifestError::InvalidCsv);
    }
    let unescaped_key = raw_key[1..raw_key.len() - 1].replace("\"\"", "\"");
    if unescaped_key.is_empty() {
        // todo: validate the key (no \0, \\, .., ., \ in the end)
        Err(ManifestError::InvalidCsv)
    } else {
        Ok(DbEntry {
            full_key: unescaped_key,
            etag: Some(tokens[1].to_owned()),
            size: Some(tokens[2].parse::<usize>().map_err(|_| ManifestError::InvalidCsv)?),
        })
    }
}
