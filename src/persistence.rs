use crate::errors::Errors;
use crate::options::DharmaOpts;

/// Encapsulates all functionality that involves reading
/// and writing to File System.
pub struct Persistence<K, V> {
    options: DharmaOpts,
    key: Option<K>,
    value: Option<V>,
}

impl<K, V> Persistence<K, V> {
    pub fn create(options: &DharmaOpts) -> Result<Persistence<K, V>, Errors> {
        unimplemented!()
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, Errors> {
        // read SSTables and return the value is present
        unimplemented!()
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), Errors> {
        // write to Write Ahead Log
        let a = key;
        let b = value;
        Ok(())
    }

    pub fn delete(&mut self, key: &K) -> Result<(), Errors> {
        // add delete marker to Write Ahead Log
        unimplemented!()
    }

    pub fn flush(&mut self, values: &Vec<(K, V)>) -> Result<(), Errors> {
        // write the values to disk as an SSTable
        // delete the WAL and create a new one
        unimplemented!()
    }
}
