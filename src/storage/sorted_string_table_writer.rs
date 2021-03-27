use crate::errors::Errors;
use crate::options::DharmaOpts;
use crate::storage::block::{Block, Record, RecordType, Value, create_blocks, write_block_to_disk};
use crate::traits::{ResourceKey, ResourceValue};
use buffered_offset_reader::{BufOffsetReader, OffsetReadMut};
use log;
use serde::de::DeserializeOwned;
use std::cmp::Ordering;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Write the list of key value pairs, sorted by key to a series of SSTables on disk.
/// # Arguments
/// * _option_  - Configurations options specified as `DharmaOpts`
/// * _tuples_  - List of key value pairs sorted by key.
///
/// # Returns
/// A `Result` which is
/// - `Ok`: - Empty value
/// - `Err`: - Error type as specified by `Errors` module
pub fn write_sstable<K: ResourceKey, V: ResourceValue>(
    options: &DharmaOpts,
    tuples: &Vec<(K, V)>,
    table_number: usize,
) -> Result<PathBuf, Errors> {
    let values: Vec<Value<K, V>> = tuples
        .iter()
        .map(|tup| {
            return Value::new(tup.0.clone(), tup.1.clone());
        })
        .collect();
    // pack values into blocks
    let mut blocks = Vec::new();
    // pack the values into blocks of fixed size as specified by `options.block_size_in_bytes`
    create_blocks(options, &values, &mut blocks);
    // write this chunk to disk
    let path_str = format!("{0}/tables/{1}.db", options.path, table_number);
    let path = Path::new(&path_str);
    // create file for SSTable
    let file_result = File::create(&path);
    if file_result.is_ok() {
        let mut file = file_result.unwrap();
        // write all blocks to SSTable file
        for (block_counter, block) in blocks.iter().enumerate() {
            let write_result = write_block_to_disk(options, &mut file, &block);
            if write_result.is_err() {
                log::error!(
                    "Failed to write block from chunk {0} to disk",
                    block_counter
                );
                return Err(Errors::SSTABLE_CREATION_FAILED);
            }
        }
    } else {
        log::error!("Failed to create SSTable from chunk from values");
        return Err(Errors::SSTABLE_CREATION_FAILED);
    }
    Ok(PathBuf::from(path_str))
}

/// Read the SSTable at the specified path and return the data persisted in it
/// as a `Vec` of `Value<K, V>`.
/// TODO(@deprecated) - Use SSTableReader instead.
///
/// # Arguments
/// * _option_ - Configuration options specified as `DharmaOpts`
/// * _path_ - File System Path to SSTable
///
/// # Returns
/// A `Result` that is
///  - `Ok`: The list of `Value<K, V>` persisted to the SSTable
///  - `Err`: Error type as specified by `Errors` module
pub fn read_sstable<K: DeserializeOwned, V: DeserializeOwned>(
    options: &DharmaOpts,
    path: &Path,
) -> Result<Vec<Value<K, V>>, Errors> {
    let mut output: Vec<Value<K, V>> = Vec::new();
    let file_result = File::open(path);
    if file_result.is_ok() {
        let file = file_result.unwrap();
        let metadata = file.metadata().unwrap();
        let total_size_in_bytes = metadata.len();
        // read number of blocks from metadata embedded in SStable rather than relying
        // on options. options might change which might cause read error due to mismatch
        // between written and supplied block size
        let block_count =
            (total_size_in_bytes as f64 / options.block_size_in_bytes as f64).ceil() as u64;
        let mut i = 0;
        let mut reader = BufOffsetReader::new(file);
        // buffer to accumulate data from records split across multiple blocks
        let mut record_byte_buffer = Vec::new();
        while i < block_count {
            let mut buffer = vec![0u8; options.block_size_in_bytes as usize];
            // read blocksize number of bytes
            // TODO: handle read error
            reader.read_at(&mut buffer, i * options.block_size_in_bytes as u64);
            // unpack bytes array into records
            let mut r = 0;
            while r < buffer.len() {
                let record_type = buffer[r];
                match record_type {
                    // padding record
                    0 => {
                        let remaining = buffer.len() - r;
                        if remaining <= Record::RECORD_BASE_SIZE_IN_BYTES {
                            r += remaining;
                        } else {
                            let upper_size_byte = buffer[r + 1] as u16;
                            let lower_size_byte = buffer[r + 2] as u16;
                            let size = (upper_size_byte << 8 | lower_size_byte) as usize;
                            // skip record type byte(1) and size bytes(2)
                            r += 3;
                            // skip bytes specified by padding
                            r += size;
                        }
                    }
                    // complete record
                    1 => {
                        // read size
                        let upper_size_byte = buffer[r + 1] as u16;
                        let lower_size_byte = buffer[r + 2] as u16;
                        let size = (upper_size_byte << 8 | lower_size_byte) as usize;
                        // skip record type byte(1) and size bytes(2)
                        r += 3;
                        // read size bytes
                        let decoded: Value<K, V> =
                            bincode::deserialize(&buffer[r..r + size]).unwrap();
                        output.push(decoded);
                        r += size;
                    }
                    // start and middle records
                    2 | 3 => {
                        let upper_size_byte = buffer[r + 1] as u16;
                        let lower_size_byte = buffer[r + 2] as u16;
                        let size = (upper_size_byte << 8 | lower_size_byte) as usize;
                        // skip record type byte(1) and size bytes(2)
                        r += 3;
                        for i in 0..size {
                            record_byte_buffer.push(buffer[r + i]);
                        }
                        r += size;
                    }
                    // end
                    4 => {
                        let upper_size_byte = buffer[r + 1] as u16;
                        let lower_size_byte = buffer[r + 2] as u16;
                        let size = (upper_size_byte << 8 | lower_size_byte) as usize;
                        // skip record type byte(1) and size bytes(2)
                        r += 3;
                        for i in 0..size {
                            record_byte_buffer.push(buffer[r + i]);
                        }
                        let decoded: Value<K, V> =
                            bincode::deserialize(record_byte_buffer.as_slice()).unwrap();
                        output.push(decoded);
                        r += size;
                        // last chunk in record processed so create a new buffer
                        record_byte_buffer = Vec::new();
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        return Ok(output);
    }
    Err(Errors::SSTABLE_READ_FAILED)
}
