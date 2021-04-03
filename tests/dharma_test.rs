use crate::common::test_key::TestKey;
use crate::common::test_value::TestValue;
use crate::common::{cleanup_paths, get_test_data};
use dharma::dharma::Dharma;
use dharma::errors::Errors;
use dharma::options::DharmaOpts;

mod common;

#[test]
fn test_create_database() {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    let db: Result<Dharma<TestKey, TestValue>, Errors> = Dharma::create(options);
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
#[ignore]
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

    // test reading values back
    for (key, value) in expected_data {
        let get_result = db.get(&key);
        assert!(get_result.is_ok());
        let maybe_get_value = get_result.unwrap();
        assert!(maybe_get_value.is_some());
        assert_eq!(maybe_get_value.unwrap(), value);
    }
}
