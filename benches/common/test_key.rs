use dharma::traits::ResourceKey;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter, Result};

#[derive(Serialize, Deserialize, Debug)]
pub struct TestKey {
    data: u32,
}
impl TestKey {
    pub fn from(data: u32) -> TestKey {
        TestKey { data }
    }
}

impl ResourceKey for TestKey {}

impl Clone for TestKey {
    fn clone(&self) -> Self {
        return TestKey::from(self.data);
    }
}

impl Display for TestKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.data)
    }
}

impl PartialOrd for TestKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.data.cmp(&other.data))
    }
}

impl PartialEq for TestKey {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Ord for TestKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
    }
}

impl Eq for TestKey {}
