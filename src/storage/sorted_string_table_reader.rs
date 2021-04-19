use crate::errors::{Errors, Result};
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
    pub fn to_record<K: ResourceKey, V: ResourceValue>(&self) -> Result<Value<K, V>> {
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
    pub size: usize,
    // data buffered from the current block being read
    pub buffer: Vec<u8>,
    // offset within the current buffer
    buffer_offset: usize,
    // the size of blocks in this SSTable
    block_size: usize,
    // the reader to read data in blocks
    reader: BufOffsetReader<File>,
}

impl SSTableReader {
    /// Create an SSTable reader by reading the table at the specified path
    /// with the supplied blocks size. Supplying an incorrect block size will
    /// result in reading malformed data and overflow errors.
    /// TODO(sushrut) - Encode blocksize in table metadata instead of reading from parameter
    ///
    /// # Arguments
    ///  - _path_ - The path at which the SSTable exists.
    ///  - _block_size_ - The sixe of blocks in the table. See block.rs.
    ///
    /// # Returns
    /// Result that resolves:
    ///  - _Ok_ - The SSTableReader instance.
    ///  - _Err_ - Error that occured whlie creating reader.
    pub fn from(path: &PathBuf, block_size: usize) -> Result<SSTableReader> {
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

    /// Get the paths to valid SSTables within the supplied directory.
    ///
    /// # Arguments
    ///  - _base_path_ - The directory in which to look for SSTables.
    ///
    /// # Returns
    /// Result that resolves:
    ///  - _Ok_ - The list of paths to SSTables sorted lexically.
    ///  - _Err_ - Error that occurred while reading directory.
    pub fn get_valid_table_paths(base_path: &String) -> Result<Vec<PathBuf>> {
        let tables_path = format!("{0}/tables", base_path);
        let read_dir_result = read_dir(tables_path);
        if read_dir_result.is_ok() {
            let read_dir = read_dir_result.unwrap();
            let mut output = Vec::new();
            for path_result in read_dir {
                if let Ok(dir_entry) = path_result {
                    let path = dir_entry.path();
                    let extension = path.extension();
                    if extension.is_some() && extension.unwrap().eq("db") {
                        output.push(path);
                    }
                }
            }
            output.sort();
            return Ok(output);
        }
        Err(Errors::SSTABLE_READ_FAILED)
    }

    /// Read a value from the SSTable.
    ///
    /// # Returns
    /// The value read from the SSTable.
    pub fn read(&mut self) -> SSTableValue {
        let mut previous_buffer_offset = self.buffer_offset;
        let previous_offset = self.offset;
        let previous_buffer = self.buffer.clone();
        let mut temp_buffer = Vec::new();
        loop {
            match to_record_type(self.buffer[self.buffer_offset]) {
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
                    self.buffer = previous_buffer;
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
                    self.offset = previous_offset;
                    self.buffer_offset = previous_buffer_offset;
                    self.buffer = previous_buffer;
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
    pub fn seek_closest(&mut self, offset: usize) -> Result<()> {
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
    ///
    /// # Returns
    /// Flag specifying whether more values can be read from the SSTable.
    pub fn has_next(&self) -> bool {
        if self.offset >= self.size {
            return false;
        }
        let record_type = to_record_type(self.buffer[self.buffer_offset]);
        return match record_type {
            RecordType::PADDING => {
                return self.offset + self.block_size < self.size;
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
                    break;
                }
                RecordType::COMPLETE => {
                    let upper_byte = buffer[self.buffer_offset + 1] as u16;
                    let lower_byte = buffer[self.buffer_offset + 2] as u16;
                    let size = (upper_byte << 8 | lower_byte) as usize;
                    self.buffer_offset += 3;
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
