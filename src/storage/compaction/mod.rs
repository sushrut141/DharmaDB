pub mod basic;
pub mod sparse_index;

use crate::errors::Errors;
use crate::storage::compaction::sparse_index::SparseIndex;
use std::fmt::Display;

/// Specifies the compaction strategy used to compact SSTables.
pub enum CompactionStrategy {
    /// Represents Basic compaction Strategy. See `BasicCompaction` for more details.
    BASIC,
}

/// Trait to be implemented by all Compaction strategies.
pub trait Compaction {
    /// Returns the compaction strategy being implemented.
    fn strategy(&self) -> CompactionStrategy;

    /// Method called to begin compaction process.
    fn compact<K: Ord + Display + Clone>(&self) -> Result<SparseIndex<K>, Errors>;
}
