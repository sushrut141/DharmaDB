use crate::storage::compaction::basic::errors::CompactionErrors;
use crate::storage::compaction::{Compaction, CompactionStrategy};
use crate::storage::sorted_string_table_reader::SSTableReader;
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

impl Compaction for BasicCompaction {
    fn strategy(&self) -> CompactionStrategy {
        CompactionStrategy::BASIC
    }

    fn compact<K: Ord, V>(&self) -> Result<Option<PathBuf>, CompactionErrors> {
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
                .map(SSTableReader::from)
                .map(Result::unwrap)
                .collect();
            // create new SSTable at output path
            let output_path = &self.options.output_path;
            let output_sstable_result = File::create(output_path);
            if output_sstable_result.is_ok() {
                let size = sstables.len();
                let mut output_sstable_handle = output_sstable_result.unwrap();
                let mut minimum_value = Some(sstables[0].read());
                let mut minimum_idx = 0;
                let mut valid = vec![true, size];
                loop {
                    for i in 0..size {
                        if valid[i] {
                            let current = sstables[i].read();
                            if minimum_value.is_none() {
                                minimum_value = Some(sstables[i].read());
                            }
                            match current
                                .value
                                .key
                                .cmp(&minimum_value.as_ref().unwrap().value.key)
                            {
                                Ordering::Less => {
                                    minimum_value = Some(current);
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
                                    minimum_value = Some(current);
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
                        output_sstable_handle.write(&minimum_value.as_ref().unwrap().data);
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
            return Err(CompactionErrors::INVALID_COMPACTION_OUTPUT_PATH);
        }
        Err(CompactionErrors::INVALID_COMPACTION_INPUT_PATH)
    }
}
