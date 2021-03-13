/// Configuration properties used to initialize a database instance.
pub struct DharmaOpts {
    /// Flag specifying whether to bootstrap database from existing store if data has already been persisted at that path.
    pub bootstrap: bool,
    /// Path at which data is persisted.
    pub path: String,
    /// Threshold for memtable size. If size exceeds this then memtable will be
    /// flushed to disk.
    pub memtable_size_in_bytes: usize,
    /// block size in bytes
    pub block_size_in_bytes: u64,
    /// number of blocks in an SSTable
    /// This field will be deprecated after we introduced variable sized SSTables
    pub blocks_per_sstable: u64,
    /// Sparse Index Sampling frequency. On out of all n values
    /// is stored in this spares Index
    pub sparse_index_sampling_rate: u32,
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
            memtable_size_in_bytes: 4,
            block_size_in_bytes: 32768,
            // 32 blocks (each block 32k in size) result in 1MB of memory
            // overall 32MB per SSTable
            blocks_per_sstable: 32 * 32,
            sparse_index_sampling_rate: 100,
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
        assert_eq!(options.memtable_size_in_bytes, 4);
        assert_eq!(options.block_size_in_bytes, 32768);
        assert_eq!(options.sparse_index_sampling_rate, 100);
    }
}
