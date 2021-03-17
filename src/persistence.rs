use crate::errors::Errors;
use crate::options::DharmaOpts;
use crate::sparse_index::{SparseIndex, TableAddress};
use crate::storage::sorted_string_table_reader::{SSTableReader, SSTableValue};
use crate::storage::sorted_string_table_writer::write_sstable;
use std::fmt::Display;
use std::path::PathBuf;

pub trait Persist: Clone + Display + Ord {}

/// Encapsulates all functionality that involves reading
/// and writing to File System.
pub struct Persistence<K> {
    options: DharmaOpts,
    index: SparseIndex<K>,
}

impl<K, V> Persistence<K>
where
    K: Persist,
    V: Persist,
{
    pub fn create(options: DharmaOpts) -> Result<Persistence<K>, Errors> {
        // read all SSTables and create the sparse index
        let sstable_paths = SSTableReader::get_valid_table_paths(&options.path)?;
        // read through each SSTable and create the sparse index on startup
        let mut index = SparseIndex::new();
        for path in sstable_paths {
            let load_result = Persistence::populate_index_from_path(&options, &path, &mut index);
            if load_result.is_err() {
                return Err(Errors::DB_INDEX_INITIALIZATION_FAILED);
            }
        }
        Ok(Persistence { options, index })
    }

    pub fn get(&mut self, key: &K) -> Result<Option<V>, Errors> {
        // read SSTables and return the value is present
        let maybe_address = self.index.get_nearest_address(key);
        if maybe_address.is_some() {
            let address = maybe_address.unwrap();
            let mut reader = SSTableReader::from(&address.path, self.options.block_size_in_bytes)?;
            return Ok(reader.find_from_offset(address.offset, key));
        }
        Ok(None)
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), Errors> {
        // write to Write Ahead Log
        let a = key;
        let b = value;
        Ok(())
    }

    pub fn flush(&mut self, values: &Vec<(K, V)>) -> Result<(), Errors> {
        // get the existing SSTable paths
        let paths = SSTableReader::get_valid_table_paths(&self.options.path)?;
        let flush_result = write_sstable(&self.options, values, paths.len());
        if flush_result.is_ok() {
            let new_sstable_path = flush_result.unwrap();
            //TODO: clear WAL log here
            let index_update_result = Persistence::populate_index_from_path(
                &self.options,
                &new_sstable_path,
                &mut self.index,
            );
            if index_update_result.is_err() {
                return Err(Errors::DB_INDEX_UPDATE_FAILED);
            }
            return Ok(());
        }
        Err(Errors::SSTABLE_CREATION_FAILED)
    }

    pub fn delete(&mut self, key: &K) -> Result<(), Errors> {
        // add delete marker to Write Ahead Log
        unimplemented!()
    }

    fn populate_index_from_path(
        options: &DharmaOpts,
        path: &PathBuf,
        index: &mut SparseIndex<K>,
    ) -> Result<(), Errors> {
        let mut counter = 0;
        let maybe_reader = SSTableReader::from(path, options.block_size_in_bytes);
        if maybe_reader.is_ok() {
            let mut reader = maybe_reader.unwrap();
            while reader.has_next() {
                if counter % options.sparse_index_sampling_rate == 0 {
                    let record: SSTableValue<K, V> = reader.read();
                    let key = record.value.key;
                    let offset = record.offset;
                    let address = TableAddress::new(path, offset);
                    index.update(key.clone(), address);
                }
                counter += 1;
                reader.next();
            }
            return Ok(());
        }
        Err(Errors::SSTABLE_READ_FAILED)
    }
}
