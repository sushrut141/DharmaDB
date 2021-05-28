use crate::common::get_test_data;
use crate::common::test_key::TestKey;
use crate::common::test_value::TestValue;
use dharmadb::options::DharmaOpts;
use dharmadb::storage::block::Value;
use dharmadb::storage::sorted_string_table_reader::SSTableReader;
use dharmadb::storage::sorted_string_table_writer::write_sstable;
use std::fs::File;

#[cfg(test)]
mod common;

#[test]
fn test_sstables_io() {
    let values = get_test_data(700);
    let options = DharmaOpts::default();
    let write_result = write_sstable(&options, &values, 0);
    assert!(write_result.is_ok());
    // read SSTable back
    let written_path = write_result.unwrap();
    let reader_result = SSTableReader::from(&written_path, options.block_size_in_bytes);
    assert!(reader_result.is_ok());
    let mut reader = reader_result.unwrap();
    let mut result: Vec<(TestKey, TestValue)> = Vec::new();
    let mut count = 0;
    while reader.has_next() {
        let value = reader.read().unwrap();
        let record: Value<TestKey, TestValue> =
            bincode::deserialize::<Value<TestKey, TestValue>>(&value.data).unwrap();
        result.push((record.key, record.value));
        count += 1;
        reader.next().unwrap();
    }
    assert_eq!(count, values.len());
    assert_eq!(values, result);
}

#[test]
fn test_sstable_size_is_multiple_of_block_size() {
    let values = get_test_data(800);
    let options = DharmaOpts::default();
    let write_result = write_sstable(&options, &values, 0);
    assert!(write_result.is_ok());
    let file_path = write_result.unwrap();
    let file_handle_result = File::open(&file_path);
    assert!(file_handle_result.is_ok());
    let file_handle = file_handle_result.unwrap();
    let file_size_in_bytes = file_handle.metadata().unwrap().len();
    assert_eq!(file_size_in_bytes % options.block_size_in_bytes as u64, 0);
}
