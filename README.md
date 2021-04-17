Dharma
======

Dharma is a fault tolerant, reliable key value store written in Rust.

![Build](https://github.com/sushrut141/dharma/workflows/Build/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)

## How it works

In this section we will cover the operations supported by Dharma and how  they are implemented.
Dharma supports three basic operations accessible through a REST API.

- _get_ - get the value associated with a key
- _put_ - set a value associated with a key
- _delete_ - delete value associated with a key

## Implementation

Dharma internally uses an LSM (Log Structured Memory) Tree to store and retrieve data.
LSM Trees use a combination of an in-memory sorted store along with a series of append only logs for storing the data.
By using a Write Ahead Log (WAL) we are also able to ensure fault tolerance of the database.

## Anatomy of a Write

When a value is written to the database, th following steps occur.
To ensure reliability, the data is first written to a Write Ahead Log (WAL).
The data is then inserted into an in-memory AVL tree that is sorted by key.
The initial write to the WAL ensures that is any issue occurs while persisting data
the database can reload the set of keys to be persisted from the WAL.
The in-memory AVL tree is also referred to as a memtable.

After insertion into the AVL tree, the database the value can be considered successully inserted
and the databse returns an `ok` status.

## Compaction and Persistence

When the size of the membtable grows beyond a configurable size (defaults to 5MB), the data from the membtable is persisted to disk as a segment. The in-memory sparse index and the WAL are then reset. Flushing the memtable to disk is a synchronous operation and read / writes are blocked while this occurs. If this operation fails then the WAL is backed up and an error log is printed with the details about the WAL that hasn't been backed up. A new sparse index and memtable are created to process new requests.
It is upto maintainers to ensure these backed up WALs are written to segments.
A segment is a series of keys and values stored in a block of memory in compressed form. Each segment comprises of a header and body.
The details are structured in the below format.

- header
    - key_range - The range of keys that are spanned by this segment
    - size - The size of the block in bytes
    - inserted_at - timestamp at which segment was created
- body
    - series of chunks in bytes where each chunk stores
        - size of chunk in bytes
        - compressed record in bytes

To avoid growing the number of segments to a very large number, a compaction process runs parallely that scans through all the segments and merges them to form larger segments.
The size of merged segment is configurable(default 5MB).

Once the segments are merged and compacted segments have been created, the database is notified. The database then scans through the new segments and creates a new sparse index which it switches out with its existing sparse index. The old segments are deleted in a background process.

## Sparse Index and servicing get requests

When the database is started, it looks for previously created segments and scans through them parallely.  A sparse sorted in memory index of keys is created that is used to service get requests. The sparse index is used to supply the segment and offsets between which a key might exist. For example, using the sparse index we can identify that the target key exists between keys `11234` and `11256` which are present in segment 3. We can do a linear scan between the byte offsets associated with these keys to find the target key.

For each get request, the segments and the byte offset bounds between which the key might exist in that segment are obtained by searching the sparse index.
The corresponding segment is then scanned between those bounds to look for the key.
If found, the associated value is returned else the operation returns a not ok response.


## Handling Deletes

When a key is deleted, a delete marker is appended to the write ahead log.
The key is then deleted from the sparse index and memtable if present. An in-memory list of deleted keys is also maintained. When the memtable is flushed to segment on disk, the deleted records are appended to the end of the segment with a size of 0.
During the compaction process, when these 0 size keys are encountered, they are filtered out and removed from the compacted indexes.







