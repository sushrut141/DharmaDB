# Storing Records on Disk

"The records will be converted into binary and stored on disk"

It's not as easy as it sounds. Consider the following recod that needs to be 
persisted to disk and read back into memory.
```rust
struct Record {
    key: String,
    value: String,
}
```

Disk IO always happens in blocks. A block is the smallest chunk of memory 
read when we load from disk. You specify a blocks size and 
wait for the disk to read / write the entirety of the block before yielding.
The throughput of Disk IO can  be expressed as 

`Throughput = IO per sec * Blocksize`

This can be more simply stated as the volume of data read per second is
the product number of IO operations, and the block of memory read per operation.


`IO per sec` and `Blocksize` are inversely proportional to each other. 
As the block size increases the number of IO operations that can be 
processed decreases.
The block size can be configured depending on the native block size of the 
platform. For example, Google Compute Platform has a block size of 4K.
For applications that want high IO, having a lower block size helps.
See [here](https://medium.com/@duhroach/the-impact-of-blocksize-on-persistent
-disk-performance-7e50a85b2647) for more details about block sizes.

For the purposes of reading and writing records, we can have a configurable 
block size with a default of 32K.
32K strikes a good balance, reads will not load too many records into memory 
and writes of several can occur to the same block.
We try to fit as many records as possible in 32K and add padding if there is 
any left over space.
