pub mod basic;

use crate::errors::Errors;
use std::fmt::Display;
use std::path::PathBuf;

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
    /// Returns the path to the newly created SSTable / SSTables.
    fn compact<K: Ord + Display + Clone>(&self) -> Result<Option<PathBuf>, Errors>;
}
