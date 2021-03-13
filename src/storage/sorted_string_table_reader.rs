use crate::errors::Errors;
use crate::storage::sorted_string_table_writer::Value;
use buffered_offset_reader::{BufOffsetReader, OffsetReadMut};
use serde::de::DeserializeOwned;
use std::fs::{read_dir, File};
use std::path::PathBuf;
use std::cmp::Ordering;

pub struct SSTableValue<K, V> {
    // key, value stored in the SSTable
    pub value: Value<K, V>,
    // byte array representation of the data
    pub data: Vec<u8>,
    // offset at whih this value occurs in the SSTable
    pub offset: u64,
}

// Utility to read values one after another from an SSTable.
// Use the `from` utility method to create an SSTable reader.
pub struct SSTableReader {
    // The offset at  which to read values from the SSTable
    offset: u64,
    // total size of the SSTable
    size: u64,
    // data buffered from the current bllck being read
    buffer: Vec<u8>,
    // offset within the current buffer
    buffer_offset: u64,
    // the size of blocks in this SSTable
    block_size: u64,
    // the reader to read data in blocks
    reader: BufOffsetReader<File>,
}

impl SSTableReader {
    pub fn from(path: &PathBuf, block_size: u64) -> Result<SSTableReader, Errors> {
        let file_result = File::open(path);
        if file_result.is_ok() {
            let file = file_result.unwrap();
            let size = file.metadata().unwrap().len();
            let mut reader = BufOffsetReader::new(file);
            let mut buffer = vec![0u8, block_size];
            reader.read_at(&mut buffer, 0);
            return Ok(SSTableReader {
                block_size,
                buffer,
                buffer_offset: 0,
                offset: 0,
                size,
                reader,
            });
        }
        return Err(Errors::SSTABLE_READ_FAILED);
    }

    pub fn get_valid_table_paths(input_path: &String) -> Result<Vec<PathBuf>, Errors> {
        let read_dir_result = read_dir(input_path);
        if read_dir_result.is_ok() {
            let read_dir = read_dir_result.unwrap();
            let mut output = Vec::new();
            for path_result in read_dir {
                if let Ok(dir_entry) = path_result {
                    if dir_entry.path().ends_with(".db") {
                        output.push(dir_entry.path());
                    }
                }
            }
            output.sort();
            Ok(output)
        }
        Err(Errors::SSTABLE_READ_FAILED)
    }

    /// Read the value at the current offset.
    pub fn read<K: DeserializeOwned, V: DeserializeOwned>(&mut self) -> SSTableValue<K, V> {
        let buffer = &self.buffer;
        let record_type = buffer[self.buffer_offset] as RecordType;
        let mut previous_buffer_offset = self.buffer_offset;
        let mut previous_offset = self.offset;
        loop {
            let mut temp_buffer = Vec::new();
            match record_type {
                RecordType::PADDING => {
                    self.load_next_block();
                }
                RecordType::COMPLETE => {
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    let size = upper_byte << 8 | lower_byte;
                    self.buffer_offset += 3;
                    let data_bytes = self.buffer[self.buffer_offset..(self.buffer_offset + size)];
                    let record: Value<K, V> = bincode::deserialize(data_bytes).unwrap();
                    let mut data_copy = vec![0u8, size];
                    for val in data_bytes {
                        data_copy.push(val);
                    }
                    let offset = self.offset;
                    self.offset = previous_offset;
                    self.buffer_offset = previous_buffer_offset;
                    return SSTableValue {
                        offset,
                        value: record,
                        data: data_copy,
                    };
                }
                RecordType::START | RecordType::MIDDLE => {
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    let size = upper_byte << 8 | lower_byte;
                    self.buffer_offset += 3;
                    for val in self.buffer[self.buffer_offset..(self.buffer_offset + size)] {
                        temp_buffer.push(val);
                    }
                    self.buffer_offset += size;
                    // load the next block
                    self.load_next_block();
                }
                RecordType::END => {
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    let size = upper_byte << 8 | lower_byte;
                    self.buffer_offset += 3;
                    for val in self.buffer[self.buffer_offset..(self.buffer_offset + size)] {
                        temp_buffer.push(val);
                    }
                    self.buffer_offset += size;
                    let record: Value<K, V> = bincode::deserialize(temp_buffer.as_slice()).unwrap();
                    // reset buffer and offset to previous state
                    let offset = self.offset;
                    self.load_block_at(previous_offset);
                    self.buffer_offset = previous_buffer_offset;
                    return SSTableValue {
                        offset,
                        value: record,
                        data: temp_buffer,
                    };
                }
            }
        }
    }

    /// Read the value at the specified offset.
    pub fn find_from_offset<K: DeserializeOwned + Ord, V: DeserializeOwned>(
        &mut self,
        offset: u64,
        key: &K,
    ) -> Option<SSTableValue<K, V>> {
        // get block that contains this offset
        let block_number: u64 = (offset as f64 / self.block_size as f64).floor() as u64;
        let block_offset = block_number * self.block_size;
        // load the block at this offset
        self.load_block_at(block_offset);
        self.buffer_offset = offset - block_offset;
        while self.has_next() {
            let record: SSTableValue<K, V> = self.read();
            let record_key = &record.value.key;
            match record_key.cmp(key) {
                Ordering::Less => {
                    self.next();
                }
                Ordering::Equal => {
                    return Some(record);
                }
                Ordering::Greater => {
                    return None;
                }
            }
        }
        None
    }

    /// Check whether more values can be processed in the SSTable.
    pub fn has_next(&self) -> bool {
        let record_type = self.buffer[self.buffer_offset] as RecordType;
        let buffer = &self.buffer;
        return match record_type {
            RecordType::PADDING => {
                if self.size - self.offset <= RECORD_BASE_SIZE_IN_BYTES {
                    return false;
                }
                let upper_byte = buffer[self.buffer_offset + 1] as u16;
                let lower_byte = buffer[self.buffer_offset + 2] as u16;
                let size = upper_byte << 8 | lower_byte;
                return self.offset + size < self.size;
            }
            _ => true,
        };
    }

    /// Advance the offset to the next value in the SSTable.
    /// This method should only be called if `has_next` returns `true`.
    pub fn next(&mut self) {
        loop {
            let buffer = &self.buffer;
            let record_type = self.buffer[self.buffer_offset] as RecordType;
            match record_type {
                // check if this is the last block in table before loading next block
                RecordType::PADDING => {
                    self.load_next_block();
                }
                RecordType::COMPLETE => {
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    self.buffer_offset += 3;
                    let size = upper_byte << 8 | lower_byte;
                    self.buffer_offset += size;
                    if self.buffer_offset == self.block_size {
                        self.load_next_block();
                    }
                    break;
                }
                RecordType::START | RecordType::MIDDLE => {
                    self.load_next_block();
                }
                RecordType::END => {
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    self.buffer_offset += 3;
                    let size = upper_byte << 8 | lower_byte;
                    self.buffer_offset += size;
                    if self.buffer_offset == self.block_size {
                        self.load_next_block();
                    }
                    break;
                }
            }
        }
    }

    fn load_next_block(&mut self) {
        self.load_block_at(self.offset + self.block_size);
    }

    fn load_block_at(&mut self, offset: u64) {
        let mut buffer = vec![0u8, self.block_size];
        self.offset = offset;
        self.buffer_offset = 0;
        self.reader.read_at(&mut buffer, self.offset);
        self.buffer = buffer;
    }
}
