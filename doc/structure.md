Structure of Dharma modules
---------------------------

The main Dharma interface exposes basic methods like `get`, `put` and `delete`.  
The implementation details of these operations need to be modelled as abstractions 
that encapsulate complexity.

There are three base layers we can start off with.
* Write Ahead Log
* SSTable
* Memtable

Operations to the database involve orchestrating operations on these units.

In the case of database writes
* Write to Write Ahead Log
* Write to Memtable.
* If required, flush MemTable to disk as a SSTable

In case of read
* Read Memtable
* Read SSTables across levels

During database boot-up
* Reboot from WAL if detected
* Build index from SSTable levels

During Leveled Compaction
* List number of levels
* Read list of SSTables in levels
* Merge SSTables in a level and write to next level

We can create the following abstractions to support these operations.
* SSTableReader
* SSTableWriter
* Compaction
* Write Ahead Log




