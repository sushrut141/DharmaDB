pub mod basic;

/// Specifies the compaction strategy used to compact SSTables.
pub enum CompactionStrategy {
    /// Represents Basic compaction Strategy. See `BasicCompaction` for more details.
    BASIC,
}
