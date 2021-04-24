use dharmadb::traits::{Nil, ResourceValue};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter, Result};

#[derive(Serialize, Deserialize, Debug)]
pub struct TestValue {
    data: String,
}

impl TestValue {
    pub fn from(data: &str) -> TestValue {
        TestValue {
            data: String::from(data),
        }
    }
}

impl ResourceValue for TestValue {}

impl Clone for TestValue {
    fn clone(&self) -> Self {
        return TestValue::from(self.data.as_ref());
    }
}

impl Display for TestValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.data)
    }
}

impl PartialOrd for TestValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.data.cmp(&other.data))
    }
}

impl PartialEq for TestValue {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Ord for TestValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
    }
}

impl Eq for TestValue {}

impl Nil for TestValue {
    fn nil() -> Self {
        return TestValue::from("nil");
    }
}
