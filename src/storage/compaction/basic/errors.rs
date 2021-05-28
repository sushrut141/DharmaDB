use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("Could not read SSTables from the supplied path")]
    InvalidCompactionInputPath,
    #[error("Could not write SSTables to the supplied path")]
    InvalidCompactionOutputPath,
}
