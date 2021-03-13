use serde::__private::fmt::Display;
use subway::skiplist::SkipList;

/// Represents the location of a key within an SSTable.
pub struct TableAddress {
    /// The path to the SSTable at which the target key exists.
    path: String,
    /// The byte offset into the SSTable at which to start
    /// reading for the target key.
    offset: usize,
}

pub struct SparseIndex<K> {
    data: SkipList<K, TableAddress>,
}

impl<K> SparseIndex<K>
where
    K: Ord + Clone + Display,
{
    fn new() -> SparseIndex<K> {
        SparseIndex {
            data: SkipList::new(),
        }
    }

    /// Add or update the address corresponding to the specified key.
    ///
    /// # Arguments
    /// * _key_ - The key associated with the value.
    /// * - address_ - The TableAddress specifying where the is stored.
    fn update(&mut self, key: K, address: TableAddress) {
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
    fn get_nearest_address(&self, key: K) -> TableAddress {
        //TODO: implement bisect method in skiplist to implement this method
        // bisect will return the largest value less than or equal to the supplied value
    }
}
