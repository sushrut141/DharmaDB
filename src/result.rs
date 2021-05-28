//! Errors, type aliases, and functions related to working with `Result`.
use thiserror::Error;

/// Result
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error(
        "Write Ahead log Found at supplied path. Try running \
            recovery operation to start database."
    )]
    DbPathDirty,
    #[error("No Such Key found.")]
    DbNoSuchKey,
    #[error("Could not write entry to database.")]
    DbWriteFailed,
    #[error("Could not delete entry from database.")]
    DbDeleteFailed,
    #[error("Failed to initialize sparse index for DB.")]
    DbIndexInitializationFailed,
    #[error("Failed to update the DB index during memtable flush.")]
    DbIndexUpdateFailed,
    #[error("Could not create SSTable on disk.")]
    SsTableCreationFailed,
    #[error("Failed to read SSTable from disk.")]
    SsTableReadFailed,
    #[error("Invalid read offset supplied to SSTable")]
    SsTableInvalidReadOffset,
    #[error("Failed to create Write Ahead Log during Database startup.")]
    WalLogCreationFailed,
    #[error("Write Ahead Log write failed.")]
    WalWriteFailed,
    #[error("Could not ingest existing logs to start database. Log files may be corrupted.")]
    WalBootstrapFailed,
    #[error("Failed to cleanup Write Ahead Log.")]
    WalCleanupFailed,
    #[error("Failed to serialize record.")]
    RecordSerializeationFailed,
    #[error("Failed to deserialize record.")]
    RecordDeserializeationFailed,
    #[error("Compaction cleanup failed.")]
    CompactionCleanupFailed,
    #[error(transparent)]
    CompactionError(#[from] crate::storage::compaction::basic::errors::CompactionError),
    #[error(transparent)]
    StdIoError(#[from] std::io::Error),
    #[error(transparent)]
    BincodeError(#[from] Box<bincode::ErrorKind>),
}
