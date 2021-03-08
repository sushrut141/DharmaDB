#[cfg(test)]
mod tests {
    use crate::options::DharmaOpts;
    use crate::storage::sorted_string_table::{read_sstable, write_sstables};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::path::Path;

    #[derive(Serialize, Deserialize, Debug)]
    struct TestData {
        field: u8,
        description: String,
    }

    impl TestData {
        fn new(field: u8, description: String) -> TestData {
            TestData { field, description }
        }
    }

    impl Clone for TestData {
        fn clone(&self) -> Self {
            TestData::new(self.field, self.description.clone())
        }
    }

    fn create_test_data(start: u8, end: u8) -> Vec<(u8, TestData)> {
        let mut output = Vec::new();
        for i in start..end {
            let data = TestData::new(i, String::from("data in record"));
            output.push((i, data));
        }
        output
    }

    #[test]
    fn test_sstable() {
        let data = create_test_data(0, 30);
        let mut options = DharmaOpts::default();
        options.block_size_in_bytes = 15;
        options.path = String::from("./target");
        // test writing data
        let write_result = write_sstables(&options, &data);
        print!("{:?}", write_result);
        assert!(write_result.is_ok());
        // read data from path
        let read_result =
            read_sstable::<u8, TestData>(&options, &Path::new("./target/tables/0.db"));
        assert!(read_result.is_ok());
        let values = read_result.unwrap();
        assert_eq!(values.len(), data.len());
    }
}
