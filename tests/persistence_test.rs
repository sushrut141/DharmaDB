use crate::common::test_key::TestKey;
use crate::common::test_value::TestValue;
use crate::common::{cleanup_paths, get_test_data, get_test_data_in_range};
use dharmadb::errors::Errors;
use dharmadb::options::DharmaOpts;
use dharmadb::persistence::Persistence;
use dharmadb::storage::sorted_string_table_reader::SSTableReader;
use dharmadb::traits::Nil;

mod common;

#[test]
fn test_create_persistence() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let persistence_result: Result<Persistence<TestKey>, Errors> =
        Persistence::create::<TestValue>(options);
    assert!(persistence_result.is_ok());
}

#[test]
fn test_insert_works() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let persistence_result: Result<Persistence<TestKey>, Errors> =
        Persistence::create::<TestValue>(options);
    let mut persistence = persistence_result.unwrap();
    let key = TestKey::from(1);
    let value = TestValue::from("Test Value");
    let insert_result = persistence.insert(key, value);
    assert!(insert_result.is_ok());
}

#[test]
fn test_flush_to_disk_works() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let data = get_test_data(200);
    let persistence_result: Result<Persistence<TestKey>, Errors> =
        Persistence::create::<TestValue>(options);
    let mut persistence = persistence_result.unwrap();
    let flush_result = persistence.flush(&data);
    assert!(flush_result.is_ok());
}

#[test]
fn test_persistence_get_after_flush() {
    let data = get_test_data(500);
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let persistence_result: Result<Persistence<TestKey>, Errors> =
        Persistence::create::<TestValue>(options);
    let mut persistence = persistence_result.unwrap();
    let flush_result = persistence.flush(&data);
    assert!(flush_result.is_ok());

    for (key, value) in data {
        let get_result: Result<Option<TestValue>, Errors> = persistence.get(&key);
        assert!(get_result.is_ok());
        let get_value = get_result.unwrap();
        assert!(get_value.is_some());
        assert_eq!(get_value.unwrap(), value);
    }
}

#[test]
fn test_persistence_get_after_flush_with_duplicate_keys() {
    let mut data = Vec::new();
    for i in 0..50 {
        data.push((TestKey::from(i), TestValue::from("old value")));
        data.push((TestKey::from(i), TestValue::from("new value")));
    }
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let persistence_result: Result<Persistence<TestKey>, Errors> =
        Persistence::create::<TestValue>(options);
    let mut persistence = persistence_result.unwrap();
    let flush_result = persistence.flush(&data);
    assert!(flush_result.is_ok());

    for i in 0..50 {
        let get_result = persistence.get::<TestValue>(&TestKey::from(i));
        assert!(get_result.is_ok());
        let maybe_value = get_result.unwrap();
        assert!(maybe_value.is_some());
        assert_eq!(maybe_value.unwrap(), TestValue::from("new value"));
    }
}

#[test]
fn test_persistence_delete_after_flush() {
    let data = get_test_data(200);
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let persistence_result: Result<Persistence<TestKey>, Errors> =
        Persistence::create::<TestValue>(options);
    let mut persistence = persistence_result.unwrap();
    let flush_result = persistence.flush(&data);
    assert!(flush_result.is_ok());

    // delete data
    let mut delete_data = Vec::new();
    for i in 0..100 {
        delete_data.push((TestKey::from(i), TestValue::nil()));
    }
    let delete_flush_result = persistence.flush(&delete_data);
    assert!(delete_flush_result.is_ok());

    // data in deleted range is empty
    for i in 0..100 {
        let get_result = persistence.get::<TestValue>(&TestKey::from(i));
        assert!(get_result.is_ok());
        let maybe_value = get_result.unwrap();
        assert!(maybe_value.is_none());
    }
    // data in non deleted range is present
    for (key, value) in get_test_data_in_range(100, 200) {
        let get_result = persistence.get::<TestValue>(&key);
        assert!(get_result.is_ok());
        let maybe_value = get_result.unwrap();
        assert!(maybe_value.is_some());
        assert_eq!(maybe_value.unwrap(), value);
    }
}

#[test]
fn test_flush_respects_compaction_threshold() {
    let options = DharmaOpts::default();
    let data_1 = get_test_data_in_range(0, 100);
    let data_2 = get_test_data_in_range(80, 300);
    let data_3 = get_test_data_in_range(280, 400);

    cleanup_paths(&options);
    let persistence_result: Result<Persistence<TestKey>, Errors> =
        Persistence::create::<TestValue>(options.clone());
    let mut persistence = persistence_result.unwrap();

    assert!(persistence.flush(&data_1).is_ok());
    assert!(persistence.flush(&data_2).is_ok());
    assert!(persistence.flush(&data_3).is_ok());

    let sstable_paths = SSTableReader::get_valid_table_paths(&options.path);
    assert!(sstable_paths.is_ok());
    assert_eq!(sstable_paths.unwrap().len(), 3);
}

#[test]
fn test_tables_are_compacted_after_threshold_is_met() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let data_1 = get_test_data_in_range(0, 100);
    let data_2 = get_test_data_in_range(80, 300);
    let data_3 = get_test_data_in_range(280, 400);
    let data_4 = get_test_data_in_range(400, 500);
    let persistence_result: Result<Persistence<TestKey>, Errors> =
        Persistence::create::<TestValue>(options.clone());
    let mut persistence = persistence_result.unwrap();

    assert!(persistence.flush(&data_1).is_ok());
    assert!(persistence.flush(&data_2).is_ok());
    assert!(persistence.flush(&data_3).is_ok());
    assert!(persistence.flush(&data_4).is_ok());

    let sstable_paths = SSTableReader::get_valid_table_paths(&options.path);
    assert!(sstable_paths.is_ok());
    assert_eq!(sstable_paths.unwrap().len(), 1);
}
