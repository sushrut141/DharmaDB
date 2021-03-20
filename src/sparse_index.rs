use std::fmt::Display;
use std::path::PathBuf;
use subway::skiplist::SkipList;

/// Represents the location of a key within an SSTable.
#[derive(Clone)]
pub struct TableAddress {
    /// The path to the SSTable at which the target key exists.
    pub path: PathBuf,
    /// The byte offset into the SSTable at which to start
    /// reading for the target key.
    pub offset: usize,
}

impl TableAddress {
    pub fn new(path: &PathBuf, offset: usize) -> TableAddress {
        TableAddress {
            path: path.clone(),
            offset,
        }
    }
}

pub struct SparseIndex<K> {
    data: SkipList<K, TableAddress>,
}

impl<K> SparseIndex<K>
where
    K: Ord + Clone + Display,
{
    pub fn new() -> SparseIndex<K> {
        SparseIndex {
            data: SkipList::new(),
        }
    }

    /// Add or update the address corresponding to the specified key.
    ///
    /// # Arguments
    /// * _key_ - The key associated with the value.
    /// * - address_ - The TableAddress specifying where the is stored.
    pub fn update(&mut self, key: K, address: TableAddress) {
        if self.data.get(&key).is_some() {
            self.data.delete(&key);
        }
        self.data.insert(key, address);
    }

    /// Returns the address of the largest key less than or equal to the target key.
    ///
    /// # Arguments
    /// * _key_ - The target key to compare against.
    ///
    /// # Result
    /// Table Address corresponding to the largest key `l_key` such that
    /// `l_key` <= `key`
    pub fn get_nearest_address(&mut self, key: &K) -> Option<TableAddress> {
        let maybe_nearest_key = self.data.bisect(key);
        return maybe_nearest_key.and_then(|nearest_key| self.data.get(&nearest_key));
    }
}
