use crate::traits::{ResourceKey, ResourceValue};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Value<K, V> {
    pub key: K,
    pub value: V,
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
        _ => RecordType::UNKNOWN
    }
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
