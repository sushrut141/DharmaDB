use crate::errors::Errors;
use crate::options::DharmaOpts;
use crate::storage::block::{create_blocks, write_block_to_disk, Block, Value};
use crate::traits::{ResourceKey, ResourceValue};
use std::fs::{remove_file, File};
use std::path::Path;

const WRITE_AHEAD_LOG_NAME: &str = "wal.log";

pub struct WriteAheadLog {
    options: DharmaOpts,
    writer: File,
}

impl WriteAheadLog {
    pub fn create(options: DharmaOpts) -> Result<WriteAheadLog, Errors> {
        let path = format!("{0}/{1}", options.path, WRITE_AHEAD_LOG_NAME);
        // check if WAL already exists
        if !Path::new(&path).exists() {
            let file_result = File::create(path);
            if file_result.is_ok() {
                let writer: File = file_result.unwrap();
                return Ok(WriteAheadLog {
                    options: options.clone(),
                    writer,
                });
            }
            return Err(Errors::WAL_LOG_CREATION_FAILED);
        }
        Err(Errors::DB_PATH_DIRTY)
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
    pub fn append<K: ResourceKey, V: ResourceValue>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), Errors> {
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

    /// Clear the Write Ahead Log of previously stored values.
    ///
    /// # Returns
    /// Result that resolves
    ///  - _Ok_ - New Write Ahead Log to be used in place of existing.
    ///  - _Err_ - Error that occurred while resetting Write Ahead Log.
    pub fn reset(&mut self) -> Result<WriteAheadLog, Errors> {
        let delete_wal_result = self.cleanup();
        if delete_wal_result.is_ok() {
            return WriteAheadLog::create(self.options.clone());
        }
        Err(Errors::WAL_LOG_CREATION_FAILED)
    }

    /// Delete the Write Ahead Log.
    ///
    /// # Returns
    /// Result that specifies
    ///  - _Ok_ - Write Ahead Log was successfully deleted.
    ///  - _Err_ -
    pub fn cleanup(&mut self) -> Result<(), Errors> {
        let path = format!("{0}/{1}", self.options.path, WRITE_AHEAD_LOG_NAME);
        let delete_wal_result = remove_file(&path);
        if delete_wal_result.is_err() {
            return Err(Errors::WAL_CLEANUP_FAILED);
        }
        Ok(())
    }
}
