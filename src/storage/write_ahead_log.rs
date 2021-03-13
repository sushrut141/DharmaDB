use crate::errors::Errors;
use crate::options::DharmaOpts;
use std::fs::File;
use std::path::Path;
use std::io::{BufWriter, Write};

#[derive(Serialize, Deserialize)]
struct WALRecord<K, V> {
    key: K,
    value: V,
}

impl<K, V> WALRecord<K, V> {

    pub fn new(key: K, value: V) -> WALRecord<K, V> {
        WALRecord {
            key,
            value,
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, Errors> {
        let serialize_result = bincode::serialize(&self);
        serialize_result.map_err(|_| Errors::WAL_WRITE_FAILED)
    }

}

/// Represents the mutations occurring to the memtable persisted on disk.
/// Used to backup the database in case of failure before memtable is flushed.
pub struct WriteAheadLog<K, V> {

    log: BufWriter<File>,
}

impl<K, V> WriteAheadLog<K, V> {
    pub fn create(options: &DharmaOpts) -> Result<WriteAheadLog<K, V>, Errors> {
        // check if log file already exists at path
        let wal_path = format!("{0}/wal.log", options.path);
        if Path::new(&wal_path).exists() {
            return Err(Errors::DB_PATH_DIRTY);
        }
        // create log file
        let file = File::create(wal_path);
        if file.is_ok() {
            let writer = BufWriter::new(file.unwrap());
            WriteAheadLog {
                log: writer
            }
        }
        Err(Errors::DB_LOG_CREATION_FAILED)
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), Errors> {
        let record = WALRecord::new(key, value);
        let data = record.serialize()?;
        let write_result = self.log.write(data.as_slice());
        if write_result.is_ok() && write_result.unwrap() == data.len() {
            return Ok(());
        }
        Err(Errors::WAL_WRITE_FAILED)
    }

    pub fn delete(&self, key: K) {
        unimplemented!()
    }

    pub fn recover(&self) {
        unimplemented!()
    }
}
