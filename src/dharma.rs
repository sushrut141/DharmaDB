use std::error::Error;
use std::fmt::Display;

use subway::skiplist::SkipList;

use crate::errors::Errors;
use crate::options::DharmaOpts;

struct Dharma<K, V> {
    options: DharmaOpts,

    memory: SkipList<K, V>,
}

impl<K, V> Dharma<K, V>
where
    K: Ord + Clone + Display,
    V: Clone,
{
    pub fn new(options: DharmaOpts) -> Result<Dharma<K, V>, Errors> {
        Ok(Dharma {
            options,
            memory: SkipList::new(),
        })
    }

    pub fn get(&mut self, key: &K) -> Result<V, Errors> {
        let maybe_in_memory = self.memory.get(key);
        return maybe_in_memory.ok_or(Errors::DB_NO_SUCH_KEY);
    }

    pub fn put(&mut self, key: K, value: V) -> Result<(), Errors> {
        self.memory.insert(key.clone(), value.clone());
        Ok(())
    }

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
        let db: Result<Dharma<i32, i32>, Errors> = Dharma::new(DharmaOpts::default());
        assert_eq!(db.is_ok(), true);
    }

    #[test]
    fn test_insert() {
        let mut db = Dharma::new(DharmaOpts::default()).expect("Error creating database");
        let insert = db.put(1, 1);
        assert!(insert.is_ok());
    }

    #[test]
    fn test_get() {
        let mut db = Dharma::new(DharmaOpts::default()).expect("Error creating database");
        let insert = db.put(1, 1).expect("Failed to insert entry");
        let get = db.get(&1).expect("Failed to read value from database");
        assert_eq!(get, 1);
        let non_existent = db.get(&3);
        assert!(non_existent.is_err());
        assert_eq!(non_existent.unwrap_err(), Errors::DB_NO_SUCH_KEY);
    }
}
