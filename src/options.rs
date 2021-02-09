/// Configuration properties used to initialize a database instance.
pub struct DharmaOpts {
    /// Flag specifying whether to bootstrap database from existing store if data has already been persisted at that path.
    bootstrap: bool,
    /// Path at which data is persisted.
    path: String,
}

impl DharmaOpts {
    /// Create configuration options with default values.
    /// Default value for all configuration values are specified below.
    ///
    /// # Defaults
    ///
    /// | Property | Default Value |
    /// | :------- | :------------ |
    /// | path     | /var/lib/dharma |
    /// | bootstrap | true         |
    ///
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
