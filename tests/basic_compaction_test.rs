use crate::common::test_key::TestKey;
use crate::common::test_value::TestValue;
use crate::common::{cleanup_paths, get_test_data, get_test_data_in_range};
use dharmadb::options::DharmaOpts;
use dharmadb::storage::block::Value;
use dharmadb::storage::compaction::basic::{BasicCompaction, BasicCompactionOpts};
use dharmadb::storage::sorted_string_table_reader::SSTableReader;
use dharmadb::storage::sorted_string_table_writer::write_sstable;
use dharmadb::traits::Nil;

mod common;

#[test]
fn test_basic_compaction_with_tables_of_same_size() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let mut data_1 = get_test_data(500);
    let mut data_2 = get_test_data(500);
    let data_1_write_result = write_sstable(&options, &data_1, 0);
    let data_2_write_result = write_sstable(&options, &data_2, 1);
    assert!(data_1_write_result.is_ok());
    assert!(data_2_write_result.is_ok());

    let mut compaction_opts = BasicCompactionOpts::from(options.clone());
    // override compaction path to 2
    compaction_opts.threshold = 2;
    let compaction = BasicCompaction::new(compaction_opts);

    let compaction_result = compaction.compact::<TestKey, TestValue>();
    assert!(compaction_result.is_ok());
    let maybe_compaction_path = compaction_result.unwrap();
    assert!(maybe_compaction_path.is_some());
    let compaction_path = maybe_compaction_path.unwrap();
    // test data is sorted
    let reader_result = SSTableReader::from(&compaction_path, options.block_size_in_bytes);
    assert!(reader_result.is_ok());
    let mut reader = reader_result.unwrap();
    let mut output = Vec::new();
    while reader.has_next() {
        let value = reader.read();
        let record: Value<TestKey, TestValue> = value.to_record().unwrap();
        output.push((record.key, record.value));
        reader.next();
    }
    assert_eq!(output.len(), 500);
    data_1.append(&mut data_2);
    data_1.sort_by_key(|val| val.0.clone());
    data_1.dedup();
    assert_eq!(data_1, output);
}

#[test]
fn test_basic_compaction_with_tables_of_different_size() {
    let mut data_1 = get_test_data(200);
    let mut data_2 = get_test_data_in_range(200, 700);
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let data_1_write_result = write_sstable(&options, &data_1, 0);
    let data_2_write_result = write_sstable(&options, &data_2, 1);
    assert!(data_1_write_result.is_ok());
    assert!(data_2_write_result.is_ok());

    let mut compaction_opts = BasicCompactionOpts::from(options.clone());
    // override compaction path to 2
    compaction_opts.threshold = 2;
    let compaction = BasicCompaction::new(compaction_opts);

    let compaction_result = compaction.compact::<TestKey, TestValue>();
    assert!(compaction_result.is_ok());
    let maybe_compaction_path = compaction_result.unwrap();
    assert!(maybe_compaction_path.is_some());
    let compaction_path = maybe_compaction_path.unwrap();
    // test data is sorted
    let reader_result = SSTableReader::from(&compaction_path, options.block_size_in_bytes);
    assert!(reader_result.is_ok());
    let mut reader = reader_result.unwrap();
    let mut output = Vec::new();
    while reader.has_next() {
        let value = reader.read();
        let record: Value<TestKey, TestValue> = value.to_record().unwrap();
        output.push((record.key, record.value));
        reader.next();
    }
    assert_eq!(output.len(), 700);
    data_1.append(&mut data_2);
    data_1.sort_by_key(|val| val.0.clone());
    data_1.dedup();
    assert_eq!(data_1, output);
}

#[test]
fn test_basic_compaction_with_intersecting_values() {
    let mut data_1 = get_test_data_in_range(0, 200);
    let mut data_2 = get_test_data_in_range(80, 220);
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let data_1_write_result = write_sstable(&options, &data_1, 0);
    let data_2_write_result = write_sstable(&options, &data_2, 1);
    assert!(data_1_write_result.is_ok());
    assert!(data_2_write_result.is_ok());

    let mut compaction_opts = BasicCompactionOpts::from(options.clone());
    // override compaction path to 2
    compaction_opts.threshold = 2;
    let compaction = BasicCompaction::new(compaction_opts);
    let compaction_result = compaction.compact::<TestKey, TestValue>();
    assert!(compaction_result.is_ok());
    let maybe_compaction_path = compaction_result.unwrap();
    assert!(maybe_compaction_path.is_some());
    let compaction_path = maybe_compaction_path.unwrap();
    // test data is sorted
    let reader_result = SSTableReader::from(&compaction_path, options.block_size_in_bytes);
    assert!(reader_result.is_ok());
    let mut reader = reader_result.unwrap();
    let mut output = Vec::new();
    while reader.has_next() {
        let value = reader.read();
        let record: Value<TestKey, TestValue> = value.to_record().unwrap();
        output.push((record.key, record.value));
        reader.next();
    }
    assert_eq!(output.len(), 220);
    data_1.append(&mut data_2);
    data_1.sort_by_key(|val| val.0.clone());
    data_1.dedup();
    assert_eq!(data_1, output);
}

#[test]
fn test_basic_compaction_ignores_delete_markers() {
    let mut data_1 = get_test_data_in_range(0, 200);
    let mut data_2 = Vec::new();
    // create delete markers for values in range(0-50)
    for i in 0..50 {
        data_2.push((TestKey::from(i), TestValue::nil()));
    }
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let data_1_write_result = write_sstable(&options, &data_1, 0);
    let data_2_write_result = write_sstable(&options, &data_2, 1);
    assert!(data_1_write_result.is_ok());
    assert!(data_2_write_result.is_ok());

    let mut compaction_opts = BasicCompactionOpts::from(options.clone());
    // override compaction path to 2
    compaction_opts.threshold = 2;
    let compaction = BasicCompaction::new(compaction_opts);
    let compaction_result = compaction.compact::<TestKey, TestValue>();
    assert!(compaction_result.is_ok());
    let maybe_compaction_path = compaction_result.unwrap();
    assert!(maybe_compaction_path.is_some());
    let compaction_path = maybe_compaction_path.unwrap();
    // test data is sorted
    let reader_result = SSTableReader::from(&compaction_path, options.block_size_in_bytes);
    assert!(reader_result.is_ok());
    let mut reader = reader_result.unwrap();
    let mut output = Vec::new();
    while reader.has_next() {
        let value = reader.read();
        let record: Value<TestKey, TestValue> = value.to_record().unwrap();
        output.push((record.key, record.value));
        reader.next();
    }
    assert_eq!(output.len(), 150);
    // contains values from 50..200
    let mut i = 50;
    for (key, _) in output {
        assert_eq!(key, TestKey::from(i));
        i += 1;
    }
}
