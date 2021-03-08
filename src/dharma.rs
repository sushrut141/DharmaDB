use core::mem::size_of;
use std::fmt::Display;

use subway::skiplist::SkipList;

use crate::errors::Errors;
use crate::options::DharmaOpts;
use crate::persistence::Persistence;

/// Represents the database interface using which data can be persisted and retrieved.
///
/// # Operations
/// Dharma supports three primary operations
///
///  * _get_ - Used to retrieve a value associated with a key.
///  * _put_ - Associate the supplied key with a value.
///  * _delete_ - Delete the value associated with a key.
pub struct Dharma<K, V> {
    options: DharmaOpts,

    memory: SkipList<K, V>,

    persistence: Persistence<K, V>,

    size: usize,
}

impl<K, V> Dharma<K, V>
where
    K: Ord + Clone + Display,
    V: Clone + Display,
{
    /// Create a new instance of the database based on the supplied configuration.
    /// The configuration props are encapsulated by `DharmaOpts`.
    ///
    /// # Arguments
    /// * _options_ - The configuration properties used to initialize the database.
    ///
    /// # Example
    /// ```rust
    /// use dharma::dharma::Dharma;
    /// use dharma::options::DharmaOpts;
    ///
    /// let db_status = Dharma::create(DharmaOpts::default());
    /// if db_status.is_ok() {
    ///     let mut db: Dharma<String, i32> = db_status.unwrap();
    /// }
    /// ```
    pub fn create(options: DharmaOpts) -> Result<Dharma<K, V>, Errors> {
        let persistence_result = Persistence::create(&options);
        return persistence_result.map(|persistence| Dharma {
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
    /// # Example
    /// ```rust
    /// use dharma::dharma::Dharma;
    /// use dharma::options::DharmaOpts;
    ///
    /// let db_status = Dharma::new(DharmaOpts::default());
    /// if db_status.is_ok() {
    ///     let mut db: Dharma<String, i32> = db_status.unwrap();
    ///     let key = String::from("1234");
    ///     let result = db.get(&key);
    /// }
    /// ```
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
    /// # Example
    /// ```rust
    /// use dharma::dharma::Dharma;
    /// use dharma::options::DharmaOpts;
    ///
    /// let db_status = Dharma::new(DharmaOpts::default());
    /// if db_status.is_ok() {
    ///     let mut db: Dharma<String, i32> = db_status.unwrap();
    ///     let key = String::from("1234");
    ///     let value = 11235;
    ///     let put_status = db.put(key.clone(), value);
    ///     if put_status.is_ok() {
    ///         // value successfully inserted
    ///     }
    /// }
    /// ```
    pub fn put(&mut self, key: K, value: V) -> Result<(), Errors> {
        // try inserting into WAL else fail the operation
        // might need to acquire lock over memory before mutating memory
        let wal_insert_result = self.persistence.insert(key.clone(), value.clone());
        if wal_insert_result.is_ok() {
            self.memory.insert(key.clone(), value.clone());
            self.size += size_of::<K>() + size_of::<V>();
            // threshold exceeded so try flushing memtable to disk
            if self.size >= self.options.memtable_size_in_bytes {
                let keys_and_values: Vec<(K, V)> = self.memory.collect();
                // several things could go wrong here
                // flushing memtable to disk could fail
                // or deleting old WAL could fail
                // appropriate recovery action will have to be taken
                let flush_memory_result = self.persistence.flush(&keys_and_values);
                if flush_memory_result.is_ok() {
                    self.reset_memory();
                    return Ok(());
                }
                return flush_memory_result;
            }
        }
        Err(Errors::WAL_WRITE_FAILED)
    }

    /// Delete value associated with the supplied key if it exists.
    /// # Arguments
    /// * _key_ - The key to be removed from the database.
    ///
    /// # Example
    /// ```rust
    /// use dharma::dharma::Dharma;
    /// use dharma::options::DharmaOpts;
    ///
    /// let db_status = Dharma::new(DharmaOpts::default());
    /// if db_status.is_ok() {
    ///     let mut db: Dharma<String, i32> = db_status.unwrap();
    ///     let key = String::from("1234");
    ///     let delete_status = db.delete(&key);
    ///     if delete_status.is_ok() {
    ///         // value successfully deleted
    ///     }
    /// }
    /// ```
    pub fn delete(&mut self, key: &K) -> Result<(), Errors> {
        let wal_delete_result = self.persistence.delete(key);
        if wal_delete_result.is_ok() {
            self.memory.delete(&key);
            return Ok(());
        }
        wal_delete_result
    }

    /// In case of database crash, this operation attempts to recover
    /// the database from the Write Ahead Log. This operation may lead to
    /// data loss.
    pub fn recover(options: DharmaOpts) -> Result<Dharma<K, V>, Errors> {
        unimplemented!()
    }

    /// Create a new in-memory store to process further operations.
    /// This operation is required after the current in-memory data is flushed to disk.
    fn reset_memory(&mut self) {
        self.memory = SkipList::new();
        self.size = 0;
    }
}
