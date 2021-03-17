use std::fmt::{Debug, Display, Formatter};

#[allow(non_camel_case_types)]
#[derive(PartialEq)]
pub enum Errors {
    DB_PATH_DIRTY,
    DB_LOG_CREATION_FAILED,
    DB_BOOTSTRAP_FAILED,
    DB_NO_SUCH_KEY,
    DB_WRITE_FAILED,
    DB_DELETE_FAILED,
    DB_INDEX_INITIALIZATION_FAILED,
    DB_INDEX_UPDATE_FAILED,
    SSTABLE_CREATION_FAILED,
    SSTABLE_READ_FAILED,
    WAL_WRITE_FAILED,
}

impl Errors {
    pub fn value(&self) -> &'static str {
        match self {
            Errors::DB_PATH_DIRTY => "The supplied database path is already in use.",
            Errors::DB_LOG_CREATION_FAILED => {
                "Failed to create Write Ahead Log during Database startup."
            }
            Errors::DB_BOOTSTRAP_FAILED => {
                "Could not ingest existing logs to start database. Log files may be corrupted."
            }
            Errors::DB_NO_SUCH_KEY => "No Such Key found.",
            Errors::DB_WRITE_FAILED => "Could not write entry to database.",
            Errors::DB_DELETE_FAILED => "Could not delete entry from database.",
            Errors::SSTABLE_CREATION_FAILED => "Could not create SSTable on disk.",
            Errors::SSTABLE_READ_FAILED => "Failed to read SSTable from disk.",
            Errors::WAL_WRITE_FAILED => "Write Ahead Log write failed.",
            Errors::DB_INDEX_INITIALIZATION_FAILED => "Failed to initialize sparse index for DB",
            Errors::DB_INDEX_UPDATE_FAILED => "Failed to update the DB index during memtable flush",
        }
    }
}

impl Display for Errors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

impl Debug for Errors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}
