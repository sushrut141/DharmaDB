/// Configuration properties used to initialize a database instance.
pub struct DharmaOpts {
    /// Used to bootstrap database from existing store if data has already been persisted at that path.
    bootstrap: bool,
    /// Path at which data is persisted.
    path: String,
}

impl DharmaOpts {
    pub fn default() -> DharmaOpts {
        DharmaOpts {
            bootstrap: true,
            path: String::from("/var/lib/dharma"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let options = DharmaOpts::default();
        assert!(options.bootstrap);
        assert_eq!(options.path, String::from("/var/lib/dharma"));
    }
}
