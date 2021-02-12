use std::error::Error;
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
        Ok(Dharma {
            options,
            memory: SkipList::new(),
        })
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
    pub fn get(&mut self, key: &K) -> Result<V, Errors> {
        let maybe_in_memory = self.memory.get(key);
        return maybe_in_memory.ok_or(Errors::DB_NO_SUCH_KEY);
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
        self.memory.insert(key.clone(), value.clone());
        Ok(())
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
        self.memory.delete(&key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::dharma::Dharma;
    use crate::errors::Errors;
    use crate::options::DharmaOpts;

    #[test]
    fn test_creation() {
        let db: Result<Dharma<i32, i32>, Errors> = Dharma::create(DharmaOpts::default());
        assert_eq!(db.is_ok(), true);
    }

    #[test]
    fn test_insert() {
        let mut db = Dharma::create(DharmaOpts::default()).expect("Error creating database");
        let insert = db.put(1, 1);
        assert!(insert.is_ok());
    }

    #[test]
    fn test_get() {
        let mut db = Dharma::create(DharmaOpts::default()).expect("Error creating database");
        let insert = db.put(1, 1).expect("Failed to insert entry");
        let get = db.get(&1).expect("Failed to read value from database");
        assert_eq!(get, 1);
        let non_existent = db.get(&3);
        assert!(non_existent.is_err());
        assert_eq!(non_existent.unwrap_err(), Errors::DB_NO_SUCH_KEY);
    }
}
