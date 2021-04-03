use crate::common::test_key::TestKey;
use crate::common::test_value::TestValue;
use dharma::options::DharmaOpts;
use std::fs::{create_dir, remove_dir_all, remove_file};

pub mod test_key;
pub mod test_value;

pub fn get_test_data(count: u32) -> Vec<(TestKey, TestValue)> {
    let mut vector = Vec::new();
    for i in 0..count {
        let key = TestKey::from(i);
        let value = TestValue::from(format!("value is {}", i).as_str());
        vector.push((key, value));
    }
    vector
}

/// Clean any leftover log files from previous test executions.
///
/// # Arguments
///   - _options_ - The database config.
pub fn cleanup_paths(options: &DharmaOpts) {
    let sstable_dir = format!("{0}/tables", options.path);
    let wal_path = format!("{0}/wal/log", options.path);
    remove_dir_all(&sstable_dir);
    create_dir(&sstable_dir);
    remove_file(&wal_path);
}
