use crate::common::{TestKey, TestValue};
use dharma::dharma::Dharma;
use dharma::errors::Errors;
use dharma::options::DharmaOpts;

mod common;

#[test]
fn test_create_database() {
    let options = DharmaOpts::default();
    let db: Result<Dharma<TestKey, TestValue>, Errors> = Dharma::create(options);
    assert!(db.is_ok());
}
