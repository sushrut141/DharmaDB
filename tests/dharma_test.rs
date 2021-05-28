use crate::common::test_key::TestKey;
use crate::common::test_value::TestValue;
use crate::common::{cleanup_paths, get_test_data, get_test_data_in_range};
use dharmadb::dharma::Dharma;
use dharmadb::result::{Error, Result};
use dharmadb::options::DharmaOpts;
use dharmadb::storage::write_ahead_log::WriteAheadLog;

mod common;

#[test]
fn test_create_database() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let db: Result<Dharma<TestKey, TestValue>> = Dharma::create(options);
    assert!(db.is_ok());
}

#[test]
fn test_insert_and_get() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);

    let mut db: Dharma<TestKey, TestValue> = Dharma::create(options).unwrap();
    let insert_result = db.put(TestKey::from(1), TestValue::from("first value"));

    assert!(insert_result.is_ok());
    let query_result = db.get(&TestKey::from(1));
    assert!(query_result.is_ok());
    let maybe_result = query_result.unwrap();
    assert_eq!(maybe_result, Some(TestValue::from("first value")));
}

#[test]
fn test_delete() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);

    let mut db: Dharma<TestKey, TestValue> = Dharma::create(options).unwrap();
    let key = TestKey::from(1);
    let insert_result = db.put(key.clone(), TestValue::from("first value"));
    assert!(insert_result.is_ok());
    // delete value associated with key
    let delete_result = db.delete(key.clone());
    assert!(delete_result.is_ok());

    // value should not be retrievable after delete
    let get_result = db.get(&key);
    assert!(get_result.is_ok());
    assert_eq!(get_result, Ok(None));
}

#[test]
fn test_database_flush() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let data = get_test_data(50);
    let mut db: Dharma<TestKey, TestValue> = Dharma::create(options).unwrap();
    for (key, value) in data {
        db.put(key, value);
    }
    let flush_result = db.flush();
    assert!(flush_result.is_ok());
}

#[test]
fn test_database_operations_after_flush() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let data = get_test_data(200);
    let expected_data = data.clone();
    let mut db: Dharma<TestKey, TestValue> = Dharma::create(options).unwrap();
    for (key, value) in data {
        db.put(key, value);
    }
    // flush database to ensure no data remains in memory
    let flush_result = db.flush();
    assert!(flush_result.is_ok());
    assert_eq!(db.in_memory_size(), 0);
    // test reading values back
    for (key, value) in expected_data {
        let get_result = db.get(&key);
        assert!(get_result.is_ok());
        let maybe_get_value = get_result.unwrap();
        assert!(maybe_get_value.is_some());
        assert_eq!(maybe_get_value.unwrap(), value);
    }
}

#[test]
fn test_database_delete_after_flush() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let data = get_test_data(10);
    let expected_data = data.clone();
    let mut db: Dharma<TestKey, TestValue> = Dharma::create(options).unwrap();
    for (key, value) in data {
        db.put(key, value);
    }
    // delete keys in range of 0..5
    for i in 0..5 {
        assert!(db.delete(TestKey::from(i)).is_ok());
    }
    db.flush();
    // data in delete range should return null
    for i in 0..5 {
        let get_result = db.get(&TestKey::from(i));
        assert!(get_result.is_ok());
        let maybe_value = get_result.unwrap();
        assert!(maybe_value.is_none());
    }
    // data in non deleted range should exist
    for (key, value) in get_test_data_in_range(5, 10) {
        let get_result = db.get(&key);
        assert!(get_result.is_ok());
        let maybe_value = get_result.unwrap();
        assert!(maybe_value.is_some());
        assert_eq!(maybe_value.unwrap(), value);
    }
}

#[test]
fn test_database_reads_data_from_existing_sstable() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);

    let test_data_1 = get_test_data(200);
    let test_data_2 = get_test_data(200);
    let mut db = Dharma::create(options.clone()).unwrap();
    for (key, value) in test_data_1 {
        db.put(key, value);
    }
    let flush_result = db.flush();
    assert!(flush_result.is_ok());
    std::mem::drop(db);
    // initialize new database
    let mut new_db: Dharma<TestKey, TestValue> = Dharma::create(options.clone()).unwrap();
    for (key, expected_value) in test_data_2 {
        let retrieved_value = new_db.get(&key);
        assert!(retrieved_value.is_ok());
        assert_eq!(retrieved_value.unwrap().unwrap(), expected_value);
    }
}

#[test]
fn test_database_initialization_fails_when_wal_exists_at_path() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);

    let data = get_test_data(200);
    let mut wal = WriteAheadLog::create(options.clone()).unwrap();
    for (key, value) in data {
        wal.append(key, value);
    }
    // initializing database should fail due to exustence of wal
    let mut db_result: Result<Dharma<TestKey, TestValue>> = Dharma::create(options.clone());
    assert!(db_result.is_err());
}

#[test]
fn test_database_recovery_from_existing_wal() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);

    let data = get_test_data(200);
    let expected_data = get_test_data(200);
    let mut wal = WriteAheadLog::create(options.clone()).unwrap();
    for (key, value) in data {
        wal.append(key, value);
    }
    // initializing database should fail due to exustence of wal
    let mut db_result: Result<Dharma<TestKey, TestValue>> = Dharma::create(options.clone());
    assert!(db_result.is_err());
    // attempt database recovery
    let new_db_result = Dharma::<TestKey, TestValue>::recover::<TestKey, TestValue>(options);
    assert!(new_db_result.is_ok());
    let mut new_db = new_db_result.unwrap();
    for (key, expected_value) in expected_data {
        let value_result = new_db.get(&key);
        assert!(value_result.is_ok());
        let maybe_value = value_result.unwrap();
        assert!(maybe_value.is_some());
        assert_eq!(maybe_value.unwrap(), expected_value);
    }
}
