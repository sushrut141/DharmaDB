Basic Compaction
======================

Data written to the database is initially written to an in-memory store 
called the memtable. The memtable is flushed to disk when the amount of data 
crosses a threshold. The data flushed to disk is written as an SSTable.

As more data is written, he number of tables on disk will keep increasing.
Since the search performance depends on the number of SSTables to be searched, 
we need to limit the number of SSTables.
We do this by running the compaction service as the number of SSTables grow.
When the number of SSTables grows equals a configurable limit, we merge 
them into a single table. On every SSTable flush, we check to see if the 
number of tables equals the limit, if so block till the tables are merged 
into a single table.

In workloads where data is frequently updated, the merged SSTable size may be 
far lower than the number of SStables * size of SStable. For now, we will 
move forward without taking this case into consideration.

## Handling updates to the live index
`get` operation involve identifying the correct SSTable to query and then 
searching for the existence of a value in it.
To speed up `get` operation, an in-memory sparse index of key to the SSTable, and
the offset within it is maintained in memory.
```rustc
struct TableAddress {
    // the idx of the SSTable housing the data
    table: usize,
    // the offset within the SSTable at which the key can be found
    offset: usize
}
let key: L = /**  A key in the table **/
let index = HashMap<K, TableAddress>::new();
// ...
// ...
// populate index
// ..
// ...
let address = index.get(key);
```
The `TableAddress` corresponding to a key is used to seek to the correct 
offset within an SSTable and begin searching for a key. Having this offset 
avoids us having to search all SSTables / the entire SSTable. 
To full-fill, a get request the index is queried to find the key or the 
largest value less than the queried key. The `TableAddress` corresponding to 
that key is used to read the SSTable.

The Sparse Index needs to kept in sync with the state of SSTables on disk.
As a result, every compaction is returns a new Sparse Index that can replace
the existing index.

## Updates to Sparse Index on Memtable Flush
When the memtable is flushed, it leads to creation of another SSTable on disk.
The data from this table has not yet been incorporated into the Sparse Index 
so, searching for a key would require a linear pass over all new SSTables.
To avoid this, when a memtable is flushed to disk, the keys that have been 
overwritten are updated in the Sparse Index to point to the new `TableAddress`.
Also, new keys are sampled and corresponding entries are added to the Sparse 
Index as well. Eventually, these newly flushed tables are compacted into a 
single table, and the sparse index is updated.





