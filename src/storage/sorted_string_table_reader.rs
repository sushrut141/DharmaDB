use crate::errors::Errors;
use crate::storage::block::{to_record_type, Record, RecordType, Value};
use crate::traits::{ResourceKey, ResourceValue};
use buffered_offset_reader::{BufOffsetReader, OffsetReadMut};
use std::fs::{read_dir, File};
use std::path::PathBuf;

pub struct SSTableValue {
    // byte array representation of the data
    pub data: Vec<u8>,
    // offset at which this value occurs in the SSTable
    pub offset: usize,
}

impl SSTableValue {
    pub fn to_record<K: ResourceKey, V: ResourceValue>(&self) -> Result<Value<K, V>, Errors> {
        let value_result = bincode::deserialize::<Value<K, V>>(self.data.as_slice());
        return value_result.map_err(|err| Errors::RECORD_DESERIALIZATION_FAILED);
    }
}

// Utility to read values one after another from an SSTable.
// Use the `from` utility method to create an SSTable reader.
pub struct SSTableReader {
    // The offset at  which to read values from the SSTable
    offset: usize,
    // total size of the SSTable
    size: usize,
    // data buffered from the current bllck being read
    buffer: Vec<u8>,
    // offset within the current buffer
    buffer_offset: usize,
    // the size of blocks in this SSTable
    block_size: usize,
    // the reader to read data in blocks
    reader: BufOffsetReader<File>,
}

impl SSTableReader {
    pub fn from(path: &PathBuf, block_size: usize) -> Result<SSTableReader, Errors> {
        let file_result = File::open(path);
        if file_result.is_ok() {
            let file = file_result.unwrap();
            let size = file.metadata().unwrap().len();
            let mut reader = BufOffsetReader::new(file);
            let mut buffer = vec![0u8; block_size as usize];
            reader.read_at(&mut buffer, 0);
            return Ok(SSTableReader {
                block_size,
                buffer,
                buffer_offset: 0,
                offset: 0,
                size: size as usize,
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
            return Ok(output);
        }
        Err(Errors::SSTABLE_READ_FAILED)
    }

    /// Read the value at the current offset.
    pub fn read(&mut self) -> SSTableValue {
        let record_type = to_record_type(self.buffer[self.buffer_offset]);
        let mut previous_buffer_offset = self.buffer_offset;
        let previous_offset = self.offset;
        loop {
            let mut temp_buffer = Vec::new();
            match record_type {
                RecordType::PADDING => {
                    self.load_next_block();
                }
                RecordType::COMPLETE => {
                    let buffer = &self.buffer;
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    let size = (upper_byte << 8 | lower_byte) as usize;
                    self.buffer_offset += 3;
                    let mut data_copy = vec![0u8; size];
                    data_copy.copy_from_slice(
                        &self.buffer[self.buffer_offset..(self.buffer_offset + size)],
                    );
                    let offset = self.offset;
                    self.offset = previous_offset;
                    self.buffer_offset = previous_buffer_offset;
                    return SSTableValue {
                        offset,
                        data: data_copy,
                    };
                }
                RecordType::START | RecordType::MIDDLE => {
                    let buffer = &self.buffer;
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    let size = (upper_byte << 8 | lower_byte) as usize;
                    self.buffer_offset += 3;
                    for i in self.buffer_offset..(self.buffer_offset + size) {
                        temp_buffer.push(self.buffer[i]);
                    }
                    self.buffer_offset += size;
                    // load the next block
                    self.load_next_block();
                }
                RecordType::END => {
                    let buffer = &self.buffer;
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    let size = (upper_byte << 8 | lower_byte) as usize;
                    self.buffer_offset += 3;
                    for i in self.buffer_offset..(self.buffer_offset + size) {
                        temp_buffer.push(self.buffer[i]);
                    }
                    self.buffer_offset += size;
                    // reset buffer and offset to previous state
                    let offset = self.offset;
                    self.load_block_at(previous_offset);
                    self.buffer_offset = previous_buffer_offset;
                    return SSTableValue {
                        offset,
                        data: temp_buffer,
                    };
                }
                _ => {}
            }
        }
    }

    /// Seek the reader to the block containing the specified offset.
    ///
    /// # Returns
    /// Returns a result that is
    ///  - `()` if seek succeeded
    ///  - Err if supplied offset is invalid
    pub fn seek_closest(&mut self, offset: usize) -> Result<(), Errors> {
        if offset < self.size {
            // get block that contains this offset
            let block_number: usize = (offset as f64 / self.block_size as f64).floor() as usize;
            let block_offset = block_number * self.block_size;
            // load the block at this offset
            self.load_block_at(block_offset);
            self.buffer_offset = offset - block_offset;
            return Ok(());
        }
        Err(Errors::SSTABLE_INVALID_READ_OFFSET)
    }

    /// Check whether more values can be processed in the SSTable.
    pub fn has_next(&self) -> bool {
        if self.offset >= self.size {
            return false;
        }
        let record_type = to_record_type(self.buffer[self.buffer_offset]);
        let buffer = &self.buffer;
        return match record_type {
            RecordType::PADDING => {
                if self.size - self.offset <= Record::RECORD_BASE_SIZE_IN_BYTES {
                    return false;
                }
                let upper_byte = buffer[self.buffer_offset + 1] as u16;
                let lower_byte = buffer[self.buffer_offset + 2] as u16;
                let size = (upper_byte << 8 | lower_byte) as usize;
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
            let record_type = to_record_type(self.buffer[self.buffer_offset]);
            match record_type {
                // check if this is the last block in table before loading next block
                RecordType::PADDING => {
                    self.load_next_block();
                }
                RecordType::COMPLETE => {
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    self.buffer_offset += 3;
                    let size = (upper_byte << 8 | lower_byte) as usize;
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
                    let size = (upper_byte << 8 | lower_byte) as usize;
                    self.buffer_offset += size;
                    if self.buffer_offset == self.block_size {
                        self.load_next_block();
                    }
                    break;
                }
                _ => {}
            }
        }
    }

    fn load_next_block(&mut self) {
        self.load_block_at(self.offset + self.block_size);
    }

    fn load_block_at(&mut self, offset: usize) {
        let mut buffer = vec![0u8; self.block_size];
        self.offset = offset;
        self.buffer_offset = 0;
        self.reader.read_at(&mut buffer, self.offset as u64);
        self.buffer = buffer;
    }
}
