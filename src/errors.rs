use std::fmt::{Debug, Display, Formatter};

#[derive(PartialEq)]
pub enum Errors {
    DB_PATH_DIRTY,
    DB_BOOTSTRAP_FAILED,
    DB_NO_SUCH_KEY,
    DB_WRITE_FAILED,
    DB_DELETE_FAILED,
}

impl Errors {
    pub fn value(&self) -> &'static str {
        match self {
            DB_PATH_DIRTY => "The supplied database path is not empty.",
            DB_BOOTSTRAP_FAILED => {
                "Could not ingest existing logs to start database. Log files may be corrupted."
            }
            DB_NO_SUCH_KEY => "No Such Key found.",
            DB_WRITE_FAILED => "Could not write entry to database.",
            DB_DELETE_FAILED => "Could not delete entry from database.",
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
