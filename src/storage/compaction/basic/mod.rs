use crate::storage::block::Value;
use crate::storage::compaction::basic::errors::{CompactionError, CompactionErrors};
use crate::storage::compaction::CompactionStrategy;
use crate::storage::sorted_string_table_reader::SSTableReader;
use crate::traits::{ResourceKey, ResourceValue};
use std::cmp::Ordering;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

pub mod errors;

pub struct BasicCompactionOpts {
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

/// Compact SSTables at the configured path and write the new SSTable
/// and sparse index at the configured temporary path.
/// Basi compaction reads the the SSTables at the input path and
/// compacts them into a single table that is written to the output path.
pub struct BasicCompaction {
    options: BasicCompactionOpts,
}

impl BasicCompaction {
    fn new(options: BasicCompactionOpts) -> BasicCompaction {
        BasicCompaction { options }
    }
}

impl BasicCompaction {
    fn strategy(&self) -> CompactionStrategy {
        CompactionStrategy::BASIC
    }

    fn compact<K: ResourceKey, V: ResourceValue>(
        &self,
    ) -> Result<Option<PathBuf>, CompactionError> {
        let mut count: u64 = 0;
        let input_path = &self.options.input_path;
        // list all SSTables in the directory in sorted order
        let sstable_paths_result = SSTableReader::get_valid_table_paths(input_path);
        if sstable_paths_result.is_ok() {
            let paths = sstable_paths_result.unwrap();
            if paths.len() < 2 {
                return Ok(None);
            }
            let mut sstables: Vec<SSTableReader> = paths
                .iter()
                .map(|path| SSTableReader::from(path, self.options.block_size))
                .map(Result::unwrap)
                .collect();
            // create new SSTable at output path
            let output_path = &self.options.output_path;
            let output_sstable_result = File::create(output_path);
            if output_sstable_result.is_ok() {
                let size = sstables.len();
                let mut output_sstable_handle = output_sstable_result.unwrap();
                let mut minimum_value: Option<Value<K, V>> =
                    Some(sstables[0].read().to_record().unwrap());
                let mut minimum_idx = 0;
                let mut valid: Vec<bool> = Vec::with_capacity(size);
                loop {
                    for i in 0..size {
                        if valid[i] {
                            let current = sstables[i].read();
                            let current_value: Value<K, V> = current.to_record().unwrap();
                            if minimum_value.is_none() {
                                minimum_value =
                                    Some(sstables[i].read().to_record::<K, V>().unwrap());
                            }
                            match current_value.key.cmp(&minimum_value.as_ref().unwrap().key) {
                                Ordering::Less => {
                                    minimum_value = Some(current_value);
                                    minimum_idx = i;
                                }
                                // same key appears in another SSTable that is more recent
                                Ordering::Equal => {
                                    // discard min value for older SSTable
                                    if sstables[minimum_idx].has_next() {
                                        sstables[minimum_idx].next();
                                    } else {
                                        valid[minimum_idx] = false;
                                    }
                                    minimum_value = Some(current_value);
                                    minimum_idx = i;
                                }
                                Ordering::Greater => {
                                    // noop
                                }
                            }
                        }
                    }
                    if minimum_value.is_some() {
                        count += 1;
                        // write the minimum value to output
                        let minimum = minimum_value.as_ref().unwrap();
                        let mininum_value_data = bincode::serialize(minimum).unwrap();
                        output_sstable_handle.write(mininum_value_data.as_slice());
                        // advance offset of table with minimum value
                        if sstables[minimum_idx].has_next() {
                            sstables[minimum_idx].next();
                        } else {
                            valid[minimum_idx] = false;
                        }
                        minimum_value = None;
                    } else {
                        break;
                    }
                }
                return Ok(Some(PathBuf::from(&self.options.output_path)));
            }
            return Err(CompactionError::with(
                CompactionErrors::INVALID_COMPACTION_OUTPUT_PATH,
            ));
        }
        Err(CompactionError::with(
            CompactionErrors::INVALID_COMPACTION_INPUT_PATH,
        ))
    }
}
