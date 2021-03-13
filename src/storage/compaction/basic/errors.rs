use std::fmt::{Debug, Display, Formatter};

#[allow(non_camel_case_types)]
#[derive(PartialEq)]
pub enum CompactionErrors {
    INVALID_COMPACTION_INPUT_PATH,
    INVALID_COMPACTION_OUTPUT_PATH,
}

impl CompactionErrors {
    pub fn value(&self) -> &'static str {
        match self {
            CompactionErrors::INVALID_COMPACTION_INPUT_PATH => {
                "Could not read SSTables from the supplied path"
            }
            CompactionErrors::INVALID_COMPACTION_OUTPUT_PATH => {
                "Could not write SSTables to the supplied path"
            }
        }
    }
}

impl Display for CompactionErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

impl Debug for CompactionErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}
