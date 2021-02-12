use crate::errors::Errors;
use crate::options::DharmaOpts;
use std::fs::File;

/// Represents the mutations occurring to the memtable persisted on disk.
/// Used to backup the database in case of failure before memtable is flushed.
pub struct WriteAheadLog<K, V> {
    options: DharmaOpts,

    logfile: Option<File>,
}

impl<K, V> WriteAheadLog<K, V> {
    pub fn create(options: DharmaOpts) -> Result<WriteAheadLog<K, V>, Errors> {
        unimplemented!()
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), Errors> {
        unimplemented!()
    }

    pub fn delete(&self, key: K) {
        unimplemented!()
    }

    pub fn recover(&self) {
        unimplemented!()
    }
}
