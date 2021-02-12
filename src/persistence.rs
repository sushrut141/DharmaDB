use crate::errors::Errors;
use crate::options::DharmaOpts;
use crate::storage::write_ahead_log::WriteAheadLog;

/// Encapsulates all functionality that involves reading
/// and writing to File System.
pub struct Persistence<K, V> {
    options: DharmaOpts,
    /// Write Ahead Log to recover database in case of failure.
    log: WriteAheadLog<K, V>,
}

impl<K, V> Persistence<K, V> {
    pub fn create(options: DharmaOpts) -> Result<Persistence<K, V>, Errors> {
        unimplemented!()
    }

    pub fn read(mut self, key: &K) -> Result<Option<V>, Errors> {
        // read SSTables and return the value is present
        unimplemented!()
    }

    pub fn write(&mut self, key: K, value: V) -> Result<(), Errors> {
        // write to Write Ahead Log
        unimplemented!()
    }

    pub fn flush(&mut self, values: &Vec<(K, V)>) -> Result<(), Errors> {
        // write the values to disk as an SSTable
        // delete the WAL and create a new one
        unimplemented!()
    }
}
