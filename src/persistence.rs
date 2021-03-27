use crate::errors::Errors;
use crate::options::DharmaOpts;
use crate::sparse_index::{SparseIndex, TableAddress};
use crate::storage::block::Value;
use crate::storage::sorted_string_table_reader::{SSTableReader, SSTableValue};
use crate::storage::sorted_string_table_writer::write_sstable;
use crate::storage::write_ahead_log::WriteAheadLog;
use crate::traits::{ResourceKey, ResourceValue};
use std::cmp::Ordering;
use std::path::PathBuf;

/// Encapsulates all functionality that involves reading
/// and writing to File System.
pub struct Persistence<K: ResourceKey> {
    options: DharmaOpts,
    index: SparseIndex<K>,
    log: WriteAheadLog,
}

impl<K> Persistence<K>
where
    K: ResourceKey,
{
    /// Create the persistence layer that will be used to orchestrate read / writes with the File
    /// System.
    /// # Arguments
    ///  - _option_ - The Dharma options configuration.
    ///
    /// # Returns
    /// A result that resolves:
    ///  - _Ok_ - The created persistence instance.
    ///  - _Err_ - Error encountered while creating persistence layer.
    pub fn create<V: ResourceValue>(options: DharmaOpts) -> Result<Persistence<K>, Errors> {
        // try to create write ahead log
        let log_result = WriteAheadLog::create(options.clone());
        if log_result.is_ok() {
            // read all SSTables and create the sparse index
            let sstable_paths = SSTableReader::get_valid_table_paths(&options.path)?;
            // read through each SSTable and create the sparse index on startup
            let mut index = SparseIndex::new();
            for path in sstable_paths {
                let load_result =
                    Persistence::populate_index_from_path::<V>(&options, &path, &mut index);
                if load_result.is_err() {
                    return Err(Errors::DB_INDEX_INITIALIZATION_FAILED);
                }
            }
            return Ok(Persistence {
                log: log_result.unwrap(),
                options,
                index,
            });
        }
        Err(log_result.err().unwrap())
    }

    /// Get the value associated with the specified key.
    ///
    /// # Arguments
    ///  - _key_ - The key whose value to query.
    ///
    /// # returns
    /// Result that resolves:
    ///  - _Ok_ - Optional that may contain the result value.
    ///  - _Err_ - Error that occurred while reading the value.
    pub fn get<V: ResourceValue>(&mut self, key: &K) -> Result<Option<V>, Errors> {
        // read SSTables and return the value is present
        let maybe_address = self.index.get_nearest_address(key);
        if maybe_address.is_some() {
            let address = maybe_address.unwrap();
            let mut reader = SSTableReader::from(&address.path, self.options.block_size_in_bytes)?;
            // try to find the value in the sstable
            let seek_result = reader.seek_closest(address.offset);
            // if seek offset is invalid then return errror
            // this should never happen as long as SSTables and Sparse Index are in sync
            if seek_result.is_ok() {
                while reader.has_next() {
                    let sstable_value = reader.read();
                    let record = bincode::deserialize::<Value<K, V>>(&sstable_value.data).unwrap();
                    match record.key.cmp(key) {
                        Ordering::Less => {
                            reader.next();
                        }
                        Ordering::Equal => {
                            return Ok(Some(record.value));
                        }
                        Ordering::Greater => {
                            return Ok(None);
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Associate the supplied value with the key. This operation writes the
    /// record to the Write Ahead Log so that it can be recovered in case of failure.
    ///
    /// # Arguments
    ///  - _key_ - The key.
    ///  - _value_ - The value to save associated with the key.
    ///
    /// # Returns
    /// A result that resolves:
    ///  - _Ok_ - If value was successfully saved.
    ///  - _Err_ - Error that occurred while saving value.
    pub fn insert<V: ResourceValue>(&mut self, key: K, value: V) -> Result<(), Errors> {
        let log_write_result = self.log.append(key.clone(), value.clone());
        if log_write_result.is_ok() {
            return Ok(());
        }
        Err(Errors::DB_WRITE_FAILED)
    }

    /// Flush the list of key value pairs to disk. This method assumes that list is already
    /// sorted by key and writes the list to disk as an SSTable.
    ///
    /// # Arguments
    ///  - values - List of Key-Value pairs that need to be written to disk.
    ///
    /// # Returns
    /// Result that signifies:
    ///  - _Ok_ - If values were flushed to disk successfully.
    ///  - _Err_ - Error that occurred while saving value.
    pub fn flush<V: ResourceValue>(&mut self, values: &Vec<(K, V)>) -> Result<(), Errors> {
        // get the existing SSTable paths
        let paths = SSTableReader::get_valid_table_paths(&self.options.path)?;
        let flush_result = write_sstable(&self.options, values, paths.len());
        if flush_result.is_ok() {
            let new_sstable_path = flush_result.unwrap();
            // reset Write Ahead Log
            self.log = self.log.reset()?;
            let index_update_result = Persistence::populate_index_from_path::<V>(
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

    fn populate_index_from_path<V: ResourceValue>(
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
                    let sstable_value: SSTableValue = reader.read();
                    let record: Value<K, V> =
                        bincode::deserialize(sstable_value.data.as_slice()).unwrap();
                    let key = record.key;
                    let offset = sstable_value.offset;
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
