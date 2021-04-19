use core::mem::size_of;

use subway::skiplist::SkipList;

use crate::errors::Errors;
use crate::options::DharmaOpts;
use crate::persistence::Persistence;
use crate::traits::{ResourceKey, ResourceValue};

/// Represents the database interface using which data can be persisted and retrieved.
///
/// # Operations
/// Dharma supports three primary operations
///
///  * _get_ - Used to retrieve a value associated with a key.
///  * _put_ - Associate the supplied key with a value.
///  * _delete_ - Delete the value associated with a key.
pub struct Dharma<K: ResourceKey, V: ResourceValue> {
    options: DharmaOpts,

    memory: SkipList<K, V>,

    persistence: Persistence<K>,

    size: usize,
}

impl<'a, K, V> Dharma<K, V>
where
    K: ResourceKey,
    V: ResourceValue,
{
    /// Create a new instance of the database based on the supplied configuration.
    /// The configuration props are encapsulated by `DharmaOpts`.
    ///
    /// # Arguments
    /// * _options_ - The configuration properties used to initialize the database.
    pub fn create(options: DharmaOpts) -> Result<Dharma<K, V>, Errors> {
        let persistence_result = Persistence::create::<V>(options.clone());
        return persistence_result.map(move |persistence| Dharma {
            memory: SkipList::new(),
            size: 0,
            persistence,
            options,
        });
    }

    /// Get the value associated with the supplied key.
    ///
    /// # Arguments
    /// * _key_ - The key whose value is to fetched.
    ///
    /// # Returns
    /// Result that resolves:
    ///  - _Ok_ - Optional that may contain value if found.
    ///  - _Err_ - Error specifying why read couldn't be completed.
    pub fn get(&mut self, key: &K) -> Result<Option<V>, Errors> {
        let maybe_in_memory = self.memory.get(key);
        if maybe_in_memory.is_some() {
            return Ok(maybe_in_memory);
        }
        self.persistence.get(key)
    }

    /// Associate the supplied value with the key.
    ///
    /// # Arguments
    /// * _key_ - The key used to associate the value with.
    /// * _value_ - Value to be associated with the key.
    ///
    /// # Returns
    /// Result that resolves:
    ///  - _Ok_ - () when operation succeeded.
    ///  - _Err_ - Error specifying why operation failed.
    pub fn put(&mut self, key: K, value: V) -> Result<(), Errors> {
        // try inserting into WAL else fail the operation
        // might need to acquire lock over memory before mutating memory
        let wal_insert_result = self.persistence.insert(key.clone(), value.clone());
        if wal_insert_result.is_ok() {
            self.memory.insert(key.clone(), value.clone());
            self.size += size_of::<K>() + size_of::<V>();
            // threshold exceeded so try flushing memtable to disk
            if self.size >= self.options.memtable_size_in_bytes {
                return self.flush();
            }
            return Ok(());
        }
        Err(Errors::WAL_WRITE_FAILED)
    }

    /// In case of database crash, this operation attempts to recover
    /// the database from the Write Ahead Log. This operation may lead to
    /// data loss.
    ///
    /// # Arguments
    ///  - _options_ -  The database config
    ///
    /// # Returns
    /// Result that resolves
    ///  - _Ok_ - The initialized database instance on successful recovery.
    ///  - _Err_ - The error that occured while resolving database.
    pub fn recover<T: ResourceKey, U: ResourceValue>(
        options: DharmaOpts,
    ) -> Result<Dharma<T, U>, Errors> {
        let data = Persistence::<T>::recover(options.clone())?;
        let mut db = Dharma::create(options.clone())?;
        for (key, value) in data {
            db.put(key, value);
        }
        return Ok(db);
    }

    fn delete(&mut self, key: &K) -> Result<(), Errors> {
        unimplemented!()
    }

    /// Flush the in-memory values to disk. This method is automatically called
    /// based on configurable thresholds.
    ///
    /// # Returns
    /// Result that specifies:
    ///  - _Ok_ - Values were flushed to disk successfully.
    ///  - _Err_ - Failed to flush values to disk.
    pub fn flush(&mut self) -> Result<(), Errors> {
        let flush_memory_result = self.persistence.flush(&self.memory.collect());
        if flush_memory_result.is_ok() {
            self.reset_memory();
            return Ok(());
        }
        return flush_memory_result;
    }

    /// Gets the size in bytes of data stored in-memory currently.
    ///
    /// # Returns
    /// Size in bytes of data stored in-memory.
    pub fn in_memory_size(&self) -> usize {
        self.size
    }

    /// Create a new in-memory store to process further operations.
    /// This operation is required after the current in-memory data is flushed to disk.
    fn reset_memory(&mut self) {
        self.memory = SkipList::new();
        self.size = 0;
    }
}

/// Cleanup database state before shutdown.
impl<K, V> Drop for Dharma<K, V>
where
    K: ResourceKey,
    V: ResourceValue,
{
    fn drop(&mut self) {
        self.flush();
    }
}
