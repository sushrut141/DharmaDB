#[cfg(test)]
mod tests {
    use crate::dharma::Dharma;
    use crate::errors::Errors;
    use crate::options::DharmaOpts;

    #[test]
    fn test_creation() {
        let db: Result<Dharma<i32, i32>, Errors> = Dharma::create(DharmaOpts::default());
        assert_eq!(db.is_ok(), true);
    }

    #[test]
    fn test_insert() {
        let mut db = Dharma::create(DharmaOpts::default()).expect("Error creating database");
        let insert = db.put(1, 1);
        assert!(insert.is_ok());
    }

    #[test]
    fn test_get() {
        let mut db = Dharma::create(DharmaOpts::default()).expect("Error creating database");
        let insert = db.put(1, 1).expect("Failed to insert entry");
        let get = db.get(&1).expect("Failed to read value from database");
        assert_eq!(get.unwrap(), 1);
        let non_existent = db.get(&3);
        assert!(non_existent.is_err());
        assert_eq!(non_existent.unwrap_err(), Errors::DB_NO_SUCH_KEY);
    }
}
