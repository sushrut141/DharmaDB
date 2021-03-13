Write Ahead Log
===============

The purpose of the Write Ahead Log is to write a continuous stream of data
to disk so that the database memtable can be restored in case of a crash.
The Write Ahead Log and SSTable both store data as an array of Blocks.

Each block has a default size of `32K` although this is configurable.
Each block is packed with records. Refer the section on records to 
understand the record format.




