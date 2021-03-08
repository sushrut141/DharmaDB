use crate::errors::Errors;
use crate::options::DharmaOpts;
use std::fs::File;
use std::path::Path;
use std::io::BufWriter;


/// Represents the mutations occurring to the memtable persisted on disk.
/// Used to backup the database in case of failure before memtable is flushed.
pub struct WriteAheadLog<K, V> {

    logfile: Option<BufWriter<File>>,
}

impl<K, V> WriteAheadLog<K, V> {
    pub fn create(options: &DharmaOpts) -> Result<WriteAheadLog<K, V>, Errors> {
        // check if log file already exists at path
        let wal_path = &options.path + &String::from("/wal.log");
        if Path::new(&wal_path).exists() {
            return Err(Errors::DB_PATH_DIRTY);
        }
        // create log file
        let file = File::create(wal_path);
        if file.is_ok() {
            let writer = BufWriter::new(file.unwrap());
            WriteAheadLog {
                logfile: Some(writer)
            }
        }
        Err(Errors::DB_LOG_CREATION_FAILED)
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), Errors> {
        let file = self.logfile.as_ref().unwrap();
        let key_ser = bincode::serialize(key);
        let value_ser = bincode::serialize(value);
        let value: V = bincode::deserialize(value_ser.unwrap().as_slice());
    }

    pub fn delete(&self, key: K) {
        unimplemented!()
    }

    pub fn recover(&self) {
        unimplemented!()
    }
}
