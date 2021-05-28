use crate::options::DharmaOpts;
use crate::result::Result;
use crate::traits::{ResourceKey, ResourceValue};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::File;
use std::io::Write;

#[derive(Serialize, Deserialize, Clone)]
pub struct Value<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> PartialEq for Value<K, V>
where
    K: ResourceKey,
    V: ResourceValue,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K, V> Value<K, V>
where
    K: ResourceKey,
    V: ResourceValue,
{
    pub fn new(key: K, value: V) -> Value<K, V> {
        Value { key, value }
    }
}

#[derive(Copy, Clone)]
pub enum RecordType {
    PADDING = 0,
    COMPLETE = 1,
    START = 2,
    MIDDLE = 3,
    END = 4,
    UNKNOWN = 5,
}

/// Map a unsigned byte to a Record Type.
pub fn to_record_type(val: u8) -> RecordType {
    return match val {
        0 => RecordType::PADDING,
        1 => RecordType::COMPLETE,
        2 => RecordType::START,
        3 => RecordType::MIDDLE,
        4 => RecordType::END,
        _ => RecordType::UNKNOWN,
    };
}

/// A Record represents the key, value and some metadata persisted to disk.
/// Records are written to disk as
///
/// | type (1 byte )| size (2 bytes) | data - array of u8 of length size |
///
/// The maximum size of a record is specified in `option.block_size_in_bytes`.
/// The maximum size of a record is limited to 32KB since that is the maximum
/// addressable memory with 2 bytes.
pub struct Record {
    // 1 bytes for record type
    pub record_type: RecordType,
    // 2 bytes for size
    pub data_size_in_bytes: u16,
    // can hold up to 32 kilobytes of data
    pub data: Vec<u8>,
}

impl Record {
    /// The base size in bytes required to store metadata associated with Record like
    /// record type and size.
    pub const RECORD_BASE_SIZE_IN_BYTES: usize = 3;

    /// Create a record that will be used to pad leftover space
    /// within a block. Padding records don't contain any data.
    pub fn with_padding(size: u16) -> Record {
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
pub struct Block {
    pub records: Vec<Record>,
}

impl Block {
    pub fn new() -> Block {
        Block {
            records: Vec::new(),
        }
    }

    pub fn add(&mut self, record: Record) {
        self.records.push(record);
    }
}

pub fn create_blocks<K: ResourceKey, V: ResourceValue>(
    options: &DharmaOpts,
    values: &Vec<Value<K, V>>,
    block_vec: &mut Vec<Block>,
) {
    let mut current_block = Block::new();
    let mut available_memory_in_bytes = options.block_size_in_bytes;
    let mut i = 0;
    while i < values.len() {
        let val = &values[i];
        // TODO: add logging and handle encoding error
        let encoded = bincode::serialize(val).unwrap();
        // encoded is an array of 8 bit integers (u8)
        // each value in the array takes a byte of memory
        // therefore size of array in bytes is the size of this record in bytes
        let record_size = encoded.len();
        // each record needs at has a base size to hold
        let required_record_size = Record::RECORD_BASE_SIZE_IN_BYTES + record_size;
        match available_memory_in_bytes.cmp(&required_record_size) {
            // record will be broken into chunks
            Ordering::Less => {
                // decoder should skip reading memory in block
                // if leftover data is less than Record::RECORD_BASE_SIZE_IN_BYTES
                let mut record_offset = 0;
                if available_memory_in_bytes > Record::RECORD_BASE_SIZE_IN_BYTES {
                    // flag specifying whether we are processing the first chunk of record
                    let mut is_first_chunk = true;
                    // records are broken into chunks
                    // in each iteration of this loop we process one chunk
                    while available_memory_in_bytes > Record::RECORD_BASE_SIZE_IN_BYTES {
                        available_memory_in_bytes -= Record::RECORD_BASE_SIZE_IN_BYTES;
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

pub fn write_block_to_disk(
    options: &DharmaOpts,
    file_handle: &mut File,
    block: &Block,
) -> Result<()> {
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
                for _ in 0..record.data_size_in_bytes {
                    padding_bytes.push(0u8);
                }
                file_handle.write(&final_bytes)?;
                file_handle.write(padding_bytes.as_slice())?;
                written_size_in_bytes += final_bytes.len() + padding_bytes.len();
            }
            _ => {
                let record_type = record.record_type as u8;
                let type_bytes: [u8; 1] = record_type.to_be_bytes();
                let size_bytes: [u8; 2] = record.data_size_in_bytes.to_be_bytes();
                let data_bytes: &[u8] = &record.data;
                file_handle.write(&type_bytes)?;
                file_handle.write(&size_bytes)?;
                written_size_in_bytes += 3;
                file_handle.write(data_bytes)?;
                written_size_in_bytes += data_bytes.len();
            }
        }
    }
    // add an extra padding record if the block has space
    if written_size_in_bytes < options.block_size_in_bytes as usize {
        let mut available_space_in_bytes = options.block_size_in_bytes - written_size_in_bytes;
        if available_space_in_bytes > Record::RECORD_BASE_SIZE_IN_BYTES {
            // subtract one byte for record type specifier
            available_space_in_bytes -= Record::RECORD_BASE_SIZE_IN_BYTES;
            let type_bytes: [u8; 1] = 0_i8.to_be_bytes();
            let size_bytes = (available_space_in_bytes as u16).to_be_bytes();
            let mut padding: Vec<u8> = Vec::with_capacity(available_space_in_bytes as usize);
            for _ in 0..available_space_in_bytes {
                padding.push(0u8);
            }
            // TODO: merge these file system writes into a single call and benchmark performance
            file_handle.write(&type_bytes)?;
            file_handle.write(&size_bytes)?;
            file_handle.write(padding.as_slice())?;
        } else {
            let mut padding: Vec<u8> = Vec::with_capacity(available_space_in_bytes as usize);
            for _ in 0..available_space_in_bytes {
                padding.push(0u8);
            }
            file_handle.write(padding.as_slice())?;
            //available_space_in_bytes = 0;
        }
    }
    Ok(())
}
