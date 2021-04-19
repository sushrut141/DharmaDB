mod common;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use crate::common::test_key::TestKey;
use crate::common::test_value::TestValue;
use crate::common::{cleanup_paths, get_test_data};
use dharma::dharma::Dharma;
use dharma::options::DharmaOpts;

fn dharma_db_benchmark(c: &mut Criterion) {
    let options = DharmaOpts::default();
    cleanup_paths(&options);
    // create database and put 100 values into it
    let mut db: Dharma<TestKey, TestValue> = Dharma::create(options).unwrap();
    let data = get_test_data(1000);
    for (key, value) in data {
        db.put(key, value);
    }
    // flush databse to force sstable creation
    db.flush();
    let test_key = TestKey::from(400);
    let test_value = TestValue::from("Test data string reprsentative of small to medium payloads.");
    c.bench_function("benchmark put operation", |b| {
        b.iter(|| {
            return db.put(test_key.clone(), test_value.clone()).unwrap();
        })
    });
    c.bench_function("benchmark get operation", |b| {
        b.iter(|| {
            return db.get(&test_key).unwrap();
        })
    });
}

criterion_group!(benches, dharma_db_benchmark);
criterion_main!(benches);
