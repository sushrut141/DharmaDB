use crate::options::DharmaOpts;
use crate::result::{Error, Result};
use crate::storage::block::Value;
use crate::storage::compaction::basic::errors::CompactionError;
use crate::storage::sorted_string_table_reader::SSTableReader;
use crate::storage::sorted_string_table_writer::write_sstable_at_path;
use crate::traits::{ResourceKey, ResourceValue};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};

pub mod errors;

pub struct BasicCompactionOpts {
    // the databse config
    db_options: DharmaOpts,
    /// Path at which to read SSTables from.
    pub input_path: String,
    /// Path at which to write output SSTables
    pub output_path: String,
    /// Block Size for blocks in SSTable.
    pub block_size: usize,
    /// Number of SSTables at input path after which compaction is run to
    /// merge the SSTables into a single table.
    pub threshold: u8,
}

impl BasicCompactionOpts {
    pub fn from(options: DharmaOpts) -> BasicCompactionOpts {
        BasicCompactionOpts {
            db_options: options.clone(),
            input_path: options.path.clone(),
            output_path: format!("{}/compaction/compaction.db", options.path.clone()),
            block_size: options.block_size_in_bytes,
            threshold: 4,
        }
    }
}

struct CompactionHeapNode<K, V> {
    value: Value<K, V>,
    idx: usize,
}

impl<K, V> CompactionHeapNode<K, V>
where
    K: ResourceKey,
    V: ResourceValue,
{
    pub fn new(value: Value<K, V>, idx: usize) -> CompactionHeapNode<K, V> {
        CompactionHeapNode { value, idx }
    }
}

impl<K, V> Ord for CompactionHeapNode<K, V>
where
    K: ResourceKey,
    V: ResourceValue,
{
    fn cmp(&self, other: &Self) -> Ordering {
        // two records are equal if their keys are equal
        if self.value == other.value {
            return self.idx.cmp(&other.idx);
        }
        self.value.key.cmp(&other.value.key)
    }
}

impl<K, V> Eq for CompactionHeapNode<K, V>
where
    K: ResourceKey,
    V: ResourceValue,
{
}

impl<K, V> PartialOrd for CompactionHeapNode<K, V>
where
    K: ResourceKey,
    V: ResourceValue,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, V> PartialEq for CompactionHeapNode<K, V>
where
    K: ResourceKey,
    V: ResourceValue,
{
    fn eq(&self, other: &Self) -> bool {
        if self.idx != other.idx {
            return false;
        }
        return self.value.key == other.value.key;
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
    pub fn compact<K: ResourceKey, V: ResourceValue>(&self) -> Result<Option<PathBuf>> {
        let input_path = &self.options.input_path;
        // list all SSTables in the directory in sorted order
        let sstable_paths_result = SSTableReader::get_valid_table_paths(input_path);

        if sstable_paths_result.is_ok() {
            let paths = sstable_paths_result.unwrap();
            if paths.len() < self.options.threshold as usize {
                return Ok(None);
            }
            let mut sstables: Vec<SSTableReader> = paths
                .iter()
                .map(|path| SSTableReader::from(path, self.options.block_size))
                .map(Result::unwrap)
                .collect();
            // create new SSTable at output path
            let output_path = Path::new(&self.options.output_path);
            match output_path.parent() {
                Some(parent) => {
                    if !parent.exists() {
                        create_dir_all(parent)?;
                    }
                }
                None => (),
            };
            let _ = File::create(output_path)
                .map_err(|_| CompactionError::InvalidCompactionOutputPath)?;
            let size = sstables.len();
            let mut valid: Vec<bool> = Vec::with_capacity(size);
            for _ in 0..size {
                valid.push(true);
            }
            let mut result = Vec::new();
            let mut minimums: HashMap<usize, Value<K, V>> = HashMap::new();
            // create heap to store values
            let mut heap = BinaryHeap::new();
            let mut prev_min: Option<Value<K, V>> = None;
            for i in 0..size {
                let sstable_value = sstables[i].read()?;
                let record_result = sstable_value.to_record();
                if record_result.is_ok() {
                    let record: Value<K, V> = record_result.unwrap();
                    heap.push(Reverse(CompactionHeapNode::new(record.clone(), i)));
                    minimums.insert(i, record.clone());
                    sstables[i].next()?;
                }
            }
            while !heap.is_empty() {
                let minimum_node = heap.pop().unwrap().0;
                let value = minimum_node.value.clone();
                if prev_min.is_some() {
                    let same = prev_min.as_ref().unwrap().eq(&value);
                    if same {
                        // pop previously pushed value and push in updated value
                        result.pop();
                        // push new value only if it is not a delete marker
                        if value.value.clone() != V::nil() {
                            result.push((value.key.clone(), value.value.clone()));
                        }
                    } else {
                        result.push((value.key.clone(), value.value.clone()));
                    }
                    prev_min = Some(value.clone());
                } else {
                    prev_min = Some(value.clone());
                    result.push((value.key.clone(), value.value.clone()));
                }
                // advance the sstable pointer housing the minimum value
                if sstables[minimum_node.idx].has_next() {
                    let new_sstable_value = sstables[minimum_node.idx].read()?;
                    let new_record_result = new_sstable_value.to_record();
                    if new_record_result.is_ok() {
                        let new_record: Value<K, V> = new_record_result.unwrap();
                        minimums.insert(minimum_node.idx, new_record.clone());
                        heap.push(Reverse(CompactionHeapNode::new(
                            new_record.clone(),
                            minimum_node.idx,
                        )));
                        sstables[minimum_node.idx].next()?;
                    }
                }
            }
            write_sstable_at_path(
                &self.options.db_options,
                &result,
                &PathBuf::from(&self.options.output_path),
            )?;
            return Ok(Some(PathBuf::from(&self.options.output_path)));
        }

        Err(Error::CompactionError(
            CompactionError::InvalidCompactionInputPath,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_compaction_options() {
        let dharma_opts = DharmaOpts::default();
        let compaction_opts = BasicCompactionOpts::from(dharma_opts.clone());
        assert_eq!(compaction_opts.input_path, dharma_opts.path);
        assert_eq!(
            compaction_opts.output_path,
            format!("{}/compaction/compaction.db", dharma_opts.path)
        );
        assert_eq!(compaction_opts.block_size, dharma_opts.block_size_in_bytes);
        assert_eq!(compaction_opts.threshold, 4);
    }
}
