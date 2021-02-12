SSTable Format
==============

The Sorted String tables store a block of records on disk.
The database can be viewed as a series of SSTables persisted to disk 
that can be queried to get the value corresponding to a key.

SSTables store a sorted list of key value pairs on disk.
We store keys in sorted order so that we can look at the key range on an SSTable
and decide if the target key exists within the table.
If possible we should align on a block format that allows for fast read access within 
an SSTable, possibly using binary search.

SSTables are organized into levels with all levels except the first containing 
non overlapping key ranges. It should be possible to quickly identify in which table
a key might occur in a level. To facilitate this each level also contains an index table
that defines the last key in each table, and the associated table number.

## What to store in an SSTable?
* number of records in this block
* The idx of this block
* The level of this block
* size of binary buffer  
* binary buffer of records containing values  
* map of idx of key to byte offset within binary buffer and key and value size in bytes

## How a value us retrieved from the database?
The key is first checked for inside the in-memory table. 
If not present the persistence layer is queried for they key.
The persistent storage comprises a series of SSTables each of which stores a chunk
of the values written to the table till now.
We also store a mapping of the last key in each SSTable to the SSTable path in memory so that
we can identify which SSTable to read from.
SSTables have further optimizations like bloom filter to quickly identify whether a key exists in it.
Once an SSTable is identified as having a key, the SSTable is consumed by scanning all records
in that SSTable to find the desired key. Since the scan requires a linear pass, we limit the size of SSTables
to something reasonable.
Ideally we should keep it configurable with a default size of 40MB. We should benchmark
and test what should be the actual size of the SSTable.

## Simple Compaction Strategy
There are two levels of SSTables, the newly flushed memtables and older SSTables.
The newly flushed SSTables have overlapping key-ranges, so we might have to search all these
tables to find a value. To speed this process up, we maintain a sorted index of key-range to SSTable
so that we can identify which SSTables to search for.
To avoid large number of SStables with overlapping key ranges, a compaction process runs periodically
that merges these tables with the processed SSTables to create a new set of SSTables.
After the new SSTables are written, the in memory sparse index of last key per SSTable to table numbe
is updated so that subsequent reads can be directed to the new tables.
The number of processed SSTables will keep growing as the application grows but that shouldn't
be a problem since we can easily maintain an in-memory index of 10k keys.
To reduce number of keys it is possible to increase table size.

## Communication between compaction process and readers
After compaction, once new tables have been written to memory we need to update
the index of keys to SSTables in-memory.
We can either the new index on file system and the reader can eventually start using the new index.
Alternatively, we can communicate with the reader so that the index can be updated. 





