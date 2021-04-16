use crate::errors::Errors;
use crate::options::DharmaOpts;
use crate::storage::block::Value;
use crate::storage::compaction::basic::errors::{CompactionError, CompactionErrors};
use crate::storage::compaction::CompactionStrategy;
use crate::storage::sorted_string_table_reader::SSTableReader;
use crate::traits::{ResourceKey, ResourceValue};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use crate::storage::sorted_string_table_writer::{write_sstable, write_sstable_at_path};
use std::panic::resume_unwind;

pub mod errors;

pub struct BasicCompactionOpts {
    // the databse config
    db_options: DharmaOpts,
    /// Path at which to read SSTables from.
    input_path: String,
    /// Path at which to write output SSTables
    output_path: String,
    /// Block Size for blocks in SSTable.
    block_size: usize,
    /// Number of SSTables at input path after which compaction is run to
    /// merge the SSTables into a single table.
    threshold: u8,
}

impl BasicCompactionOpts {
    pub fn from(options: DharmaOpts) -> BasicCompactionOpts {
        BasicCompactionOpts {
            db_options: options.clone(),
            input_path: options.path.clone(),
            output_path: format!("{}/compaction/compaction.db", options.path.clone()),
            block_size: options.block_size_in_bytes,
            threshold: 2,
        }
    }
}

/// Compact SSTables at the configured path and write the new SSTable
/// and sparse index at the configured temporary path.
/// Basi compaction reads the the SSTables at the input path and
/// compacts them into a single table that is written to the output path.
pub struct BasicCompaction {
    options: BasicCompactionOpts,
}

impl BasicCompaction {
    pub fn new(options: BasicCompactionOpts) -> BasicCompaction {
        BasicCompaction { options }
    }
}

impl BasicCompaction {
    fn strategy(&self) -> CompactionStrategy {
        CompactionStrategy::BASIC
    }

    pub fn compact<K: ResourceKey, V: ResourceValue>(
        &self,
    ) -> Result<Option<PathBuf>, CompactionError> {
        let mut count: u64 = 0;
        let input_path = &self.options.input_path;
        // list all SSTables in the directory in sorted order
        let sstable_paths_result = SSTableReader::get_valid_table_paths(input_path);
        if sstable_paths_result.is_ok() {
            let paths = sstable_paths_result.unwrap();
            if paths.len() < self.options.threshold as usize {
                return Err(CompactionError::with(
                    CompactionErrors::INVALID_COMPACTION_INPUT_PATH,
                ));
            }
            let mut sstables: Vec<SSTableReader> = paths
                .iter()
                .map(|path| SSTableReader::from(path, self.options.block_size))
                .map(Result::unwrap)
                .collect();
            // create new SSTable at output path
            let output_path = Path::new(&self.options.output_path);
            if output_path.parent().is_some() && !output_path.parent().unwrap().exists() {
                create_dir_all(output_path.parent().unwrap());
            }
            let output_sstable_result = File::create(output_path);
            if output_sstable_result.is_err() {
                return Err(CompactionError::with(
                    CompactionErrors::INVALID_COMPACTION_OUTPUT_PATH,
                ));
            }
            let size = sstables.len();
            let mut output_sstable_handle = output_sstable_result.unwrap();
            let mut valid: Vec<bool> = Vec::with_capacity(size);
            for i in 0..size {
                valid.push(true);
            }
            let mut result = Vec::new();
            let mut minimums: HashMap<usize, Value<K, V>> = HashMap::new();
            loop {
                let mut min_value_data = None;
                let mut minimum_value: Option<Value<K, V>> = None;
                let mut minimum_idx = 0;
                let mut i = 0;
                // populate minimums map
                while i < size {
                    if sstables[i].has_next() && !minimums.contains_key(&i) {
                        let sstable_value = sstables[i].read();
                        let value_record_result = sstable_value.to_record();
                        if value_record_result.is_ok() {
                            let value_record = value_record_result.unwrap();
                            minimums.insert(i, value_record.clone());
                            sstables[i].next();
                            if minimum_value.is_none() {
                                minimum_value = Some(value_record.clone());
                                minimum_idx = i;
                                min_value_data = Some(sstable_value.data);
                            } else {
                                match value_record.key.cmp(&minimum_value.as_ref().unwrap().key) {
                                    Ordering::Less => {
                                        minimum_value = Some(value_record.clone());
                                        minimum_idx = i;
                                    }
                                    Ordering::Equal => {
                                        // remove duplicate minimum value
                                        minimums.remove(&minimum_idx);
                                        minimum_value = Some(value_record.clone());
                                        minimum_idx = i;
                                    }
                                    Ordering::Greater => {}
                                }
                            }
                        }
                    }
                    i += 1;
                }
                // if no minimum value found break loop
                if minimum_value.is_none() {
                    break;
                }
                // store value
                let final_min = minimum_value.unwrap();
                result.push((final_min.key, final_min.value));
                // remove minimum value from map
                minimums.remove(&minimum_idx);
                min_value_data = None;
            }
            write_sstable_at_path(
                &self.options.db_options,
                &result,
                &PathBuf::from(&self.options.output_path)
            );
            return Ok(Some(PathBuf::from(&self.options.output_path)));
        }
        Err(CompactionError::with(
            CompactionErrors::INVALID_COMPACTION_INPUT_PATH,
        ))
    }
}
