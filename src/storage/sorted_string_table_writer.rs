use crate::errors::Errors;
use crate::options::DharmaOpts;
use buffered_offset_reader::{BufOffsetReader, OffsetReadMut};
use log;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::{read_dir, File};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
pub struct Value<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> Value<K, V> {
    pub fn new(key: K, value: V) -> Value<K, V> {
        Value { key, value }
    }
}

#[derive(Copy, Clone)]
enum RecordType {
    PADDING = 0,
    COMPLETE = 1,
    START = 2,
    MIDDLE = 3,
    END = 4,
}

// A Record represents the key, value and some metadata persisted to disk.
// Records are written to disk as
// ```
// | type (1 byte )| size (2 bytes) | data - array of u8 of length size |
// ```
// The maximum size of a record is specified in `option.block_size_in_bytes`.
struct Record {
    // 1 bytes for record type
    record_type: RecordType,
    // 2 bytes for size
    data_size_in_bytes: u16,
    // can hold up to 32 kilobytes of data
    data: Vec<u8>,
}

/**
* The base number of bytes required to store a record. These bytes
* are required to store metadata about the record like
* record type (START, MIDDLE, PADDING..), size etc.
*/
const RECORD_BASE_SIZE_IN_BYTES: u64 = 3;

impl Record {
    /// Create a record that will be used to pad leftover space
    /// within a block. Padding records don't contain any data.
    fn with_padding(size: u16) -> Record {
        Record {
            record_type: RecordType::PADDING,
            data_size_in_bytes: size,
            data: Vec::new(),
        }
    }
}

/// A Block is the smallest unit of memory that is read from disk.
/// Blocks are packed together to form SSTables which
/// contain data stored in the database.
/// Each block is composed of as many records as can fit in the block. If a record doesn't
/// fit into a block then it is split across multiple blocks.
struct Block {
    records: Vec<Record>,
}

impl Block {
    fn new() -> Block {
        Block {
            records: Vec::new(),
        }
    }

    fn add(&mut self, record: Record) {
        self.records.push(record);
    }
}

/// Write the list of key value pairs, sorted by key to a series of SSTables on disk.
/// # Arguments
/// * _option_  - Configurations options specified as `DharmaOpts`
/// * _tuples_  - List of key value pairs sorted by key.
///
/// # Returns
/// A `Result` which is
/// - `Ok`: - Empty value
/// - `Err`: - Error type as specified by `Errors` module
pub fn write_sstable<K: Clone + Serialize, V: Clone + Serialize>(
    options: &DharmaOpts,
    tuples: &Vec<(K, V)>,
    table_number: usize
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
        for block in blocks {
            let write_result = write_block_to_sstable(options, &mut file, &block);
            if write_result.is_err() {
                log::error!(
                    "Failed to write block from chunk {0} to disk",
                    block_counter
                );
                return Err(Errors::SSTABLE_CREATION_FAILED);
            }
        }
    } else {
        log::error!("Failed to create sstable from chunk {0}", block_counter);
        return Err(Errors::SSTABLE_CREATION_FAILED);
    }
    Ok(PathBuf::from(path_str))
}

/// Read the SSTable at the specified path and return the data persisted in it
/// as a `Vec` of `Value<K, V>`.
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
                        if remaining as u64 <= RECORD_BASE_SIZE_IN_BYTES {
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

fn create_blocks<K: Serialize, V: Serialize>(
    options: &DharmaOpts,
    values: &Vec<Value<K, V>>,
    block_vec: &mut Vec<Block>,
) {
    let mut current_block = Block::new();
    let mut available_memory_in_bytes = options.block_size_in_bytes as u64;
    let mut i = 0;
    while i < values.len() {
        let val = &values[i];
        // TODO: add logging and handle encoding error
        let encoded = bincode::serialize(val).unwrap();
        // encoded is an array of 8 bit integers (u8)
        // each value in the array takes a byte of memory
        // therefore size of array in bytes is the size of this record in bytes
        let record_size = encoded.len() as u64;
        // each record needs at has a base size to hold
        let required_record_size = RECORD_BASE_SIZE_IN_BYTES + record_size;
        match available_memory_in_bytes.cmp(&required_record_size) {
            // record will be broken into chunks
            Ordering::Less => {
                // decoder should skip reading memory in block
                // if leftover data is less than RECORD_BASE_SIZE_IN_BYTES
                let mut record_offset = 0;
                if available_memory_in_bytes > RECORD_BASE_SIZE_IN_BYTES {
                    // flag specifying whether we are processing the first chunk of record
                    let mut is_first_chunk = true;
                    // records are broken into chunks
                    // in each iteration of this loop we process one chunk
                    while available_memory_in_bytes > RECORD_BASE_SIZE_IN_BYTES {
                        available_memory_in_bytes -= RECORD_BASE_SIZE_IN_BYTES;
                        let mut record_type = RecordType::START;
                        if !is_first_chunk {
                            record_type = RecordType::MIDDLE;
                        }
                        let mut record_offset_end = record_offset + available_memory_in_bytes;
                        if record_offset_end >= record_size {
                            record_offset_end = record_size;
                            record_type = RecordType::END;
                        }
                        let mut data_chunk: Vec<u8> = Vec::new();
                        for i in record_offset..record_offset_end {
                            data_chunk.push(encoded[i]);
                        }
                        let processed_memory_in_bytes = record_offset_end - record_offset;
                        record_offset = record_offset_end;
                        let record = Record {
                            record_type,
                            data_size_in_bytes: data_chunk.len() as u16,
                            data: data_chunk,
                        };
                        current_block.add(record);
                        // depending on record type determine whether new block has to be created
                        match record_type {
                            RecordType::END => {
                                // we may not have exhausted all the space in the block
                                available_memory_in_bytes -= processed_memory_in_bytes;
                                // if we have exhausted all space then create a new block
                                if available_memory_in_bytes == 0 {
                                    block_vec.push(current_block);
                                    current_block = Block::new();
                                    available_memory_in_bytes = options.block_size_in_bytes;
                                }
                                // break since we have finished processing this value
                                i += 1;
                                break;
                            }
                            // for start and middle blocks all space has been exhausted
                            _ => {
                                block_vec.push(current_block);
                                current_block = Block::new();
                                available_memory_in_bytes = options.block_size_in_bytes;
                                is_first_chunk = false;
                            }
                        }
                    }
                } else {
                    // bytes to pad is available space minus 1 byte to store record type(PADDING)
                    let bytes_to_pad = available_memory_in_bytes - 1;
                    let padding = Record::with_padding(bytes_to_pad as u16);
                    current_block.add(padding);
                    // create new block
                    block_vec.push(current_block);
                    current_block = Block::new();
                    available_memory_in_bytes = options.block_size_in_bytes;
                }
            }
            Ordering::Equal => {
                let record = Record {
                    record_type: RecordType::COMPLETE,
                    data_size_in_bytes: record_size as u16,
                    data: encoded,
                };
                current_block.add(record);
                block_vec.push(current_block);
                current_block = Block::new();
                available_memory_in_bytes = options.block_size_in_bytes;
                i += 1;
            }
            Ordering::Greater => {
                let record = Record {
                    record_type: RecordType::COMPLETE,
                    data_size_in_bytes: record_size as u16,
                    data: encoded,
                };
                current_block.add(record);
                available_memory_in_bytes -= required_record_size;
                i += 1;
            }
        }
    }
    // blocks are added to the block list if they have no space left in them
    // and a new block with no records committed is created
    // if the current block has records in it then it represents a block
    // that is not full and hasn't been added to the block list
    if current_block.records.len() > 0 {
        block_vec.push(current_block);
    }
}

fn write_block_to_sstable(
    options: &DharmaOpts,
    file_handle: &mut File,
    block: &Block,
) -> Result<(), Errors> {
    let mut written_size_in_bytes = 0;
    for record in &block.records {
        match record.record_type {
            RecordType::PADDING => {
                // write record type byte
                let type_bytes: [u8; 1] = 0_u8.to_be_bytes();
                let size_bytes: [u8; 2] = record.data_size_in_bytes.to_be_bytes();
                let final_bytes = [type_bytes[0], size_bytes[0], size_bytes[1]];
                let mut padding_bytes: Vec<u8> =
                    Vec::with_capacity(record.data_size_in_bytes as usize);
                for i in 0..record.data_size_in_bytes {
                    padding_bytes.push(0u8);
                }
                file_handle.write(&final_bytes);
                file_handle.write(padding_bytes.as_slice());
                written_size_in_bytes += final_bytes.len() + padding_bytes.len();
            }
            _ => {
                let record_type = record.record_type as u8;
                let type_bytes: [u8; 1] = record_type.to_be_bytes();
                let size_bytes: [u8; 2] = record.data_size_in_bytes.to_be_bytes();
                let data_bytes: &[u8] = &record.data;
                file_handle.write(&type_bytes);
                file_handle.write(&size_bytes);
                written_size_in_bytes += 3;
                file_handle.write(data_bytes);
                written_size_in_bytes += data_bytes.len();
            }
        }
    }
    // add an extra padding record if the block has space
    if written_size_in_bytes < options.block_size_in_bytes as usize {
        let mut available_space_in_bytes = options.block_size_in_bytes - written_size_in_bytes;
        if available_space_in_bytes > RECORD_BASE_SIZE_IN_BYTES {
            // subtract one byte for record type specifier
            available_space_in_bytes -= RECORD_BASE_SIZE_IN_BYTES;
            let type_bytes: [u8; 1] = 0_i8.to_be_bytes();
            let size_bytes = available_space_in_bytes.to_be_bytes();
            let mut padding: Vec<u8> = Vec::with_capacity(available_space_in_bytes as usize);
            for _ in 0..available_space_in_bytes {
                padding.push(0u8);
            }
            // TODO: merge these file system writes into a single call and benchmark performance
            file_handle.write(&type_bytes);
            file_handle.write(&size_bytes);
            file_handle.write(padding.as_slice());
        } else {
            let mut padding: Vec<u8> = Vec::with_capacity(available_space_in_bytes as usize);
            for _ in 0..available_space_in_bytes {
                padding.push(0u8);
            }
            file_handle.write(padding.as_slice());
            available_space_in_bytes = 0;
        }
    }
    Ok(())
}
