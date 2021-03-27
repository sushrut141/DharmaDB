use crate::errors::Errors;
use crate::options::DharmaOpts;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use crate::traits::{ResourceKey, ResourceValue};
use crate::storage::block::{Value, Block, create_blocks, write_block_to_disk};

pub struct WriteAheadLog {
    options: DharmaOpts,
    writer: File,
}

impl WriteAheadLog {
    pub fn new(options: DharmaOpts) -> Result<WriteAheadLog, Errors> {
        let path = format!("{0}/wal.log", options.path);
        // check if WAL already exists
        if !Path::new(&path).exists() {
            let file_result = File::open(path);
            if file_result.is_ok() {
                let writer: File = file_result.unwrap();
                return Ok(WriteAheadLog {
                    options: options.clone(),
                    writer,
                });
            }
            return Err(Errors::WAL_LOG_CREATION_FAILED);
        }
        Err(Errors::WAL_LOG_CREATION_FAILED)
    }

    /// Write the key and value to the Write Ahead Log.
    ///
    /// # Arguments
    ///  - _key_: The resource key.
    ///  - _value_: The resource value
    ///
    /// # Returns
    /// Result that is:
    ///  - _Ok_ - If the record was added to the log successfully.
    ///  - _Err_ - The there was an error writing record to disk. Partial record may be written.
    pub fn append<K: ResourceKey, V: ResourceValue>(&mut self, key: K, value: V) -> Result<(),
    Errors> {
        let value = Value::new(key, value);
        // break record into blocks
        let mut blocks: Vec<Block> = Vec::new();
        create_blocks(&self.options, &vec![value], &mut blocks);
        for block in blocks {
            let write_result = write_block_to_disk(&self.options, &mut self.writer, &block);
            if write_result.is_err() {
                return Err(Errors::WAL_WRITE_FAILED);
            }
        }
        Ok(())
    }
}