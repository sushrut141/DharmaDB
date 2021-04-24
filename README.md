# DharmaDB

DharmaDB is a persistent, fault tolerant Key-Value Store written in Rust.


![Build](https://github.com/sushrut141/dharma/workflows/Build/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)

## Setup
Just create an instance of `Dharma` to get the key value store up and running.
```rust
use dharmadb::dharma::Dharma;
use dharmadb::errors::Errors;
use dharmadb::options::DharmaOpts;

// configure options such as database persistence path
// or block size using DharmaOpts or use the defaults
let options = DharmaOpts::default();
let db_result: Result<Dharma, Errors> = Dharma::new(options);

// start using database
let db = db_result.unwrap();
```

## Operations
DharmaDB supports a native Rust API and supports common operations of a
key/value store.

The API allows you to store generic Key Value pairs
using the `Dharma<K, V>` interface.

### put

The put operation is used to persist a value associated with a key to the store.
```rust
use dharmadb::dharma::Dharma;
use dharmadb::errors::Errors;
use dharmadb::options::DharmaOpts;

let options = DharmaOpts::default();
let db_result: Result<Dharma<MyKey, MyValue>, Errors> = Dharma::new(options);

// persist key / value pair
let put_result = db.put(my_key, my_value);
```

### get

The get operation retrieves the value associated with a key if it exists.
```rust
use dharmadb::dharma::Dharma;
use dharmadb::errors::Errors;
use dharmadb::options::DharmaOpts;

let options = DharmaOpts::default();
let db_result: Result<Dharma<MyKey, MyValue>, Errors> = Dharma::new(options);

// get the key if it exists
let get_result = db.get(&my_key);
let maybe_value: Option<MyValue> = get_result.unwrap();
```

### delete

The delete operation disassociates the values the supplied key.
Retrieving a deleted key resolves `None`.
```rust
use dharmadb::dharma::Dharma;
use dharmadb::errors::Errors;
use dharmadb::options::DharmaOpts;c

let options = DharmaOpts::default();
let db_result: Result<Dharma<MyKey, MyValue>, Errors> = Dharma::new(options);

// ... store data

// delete a key from the store
let delete_result = db.delete(&my_key);
```

### recover
The recover operation is required in cases of unexpected crashes.
Generally, `Dharma` will detect non-graceful exit and suggest running
recovery on startup.
```rust
use dharmadb::dharma::Dharma;
use dharmadb::errors::Errors;
use dharmadb::options::DharmaOpts;

let options = DharmaOpts::default();
// try recovering data after non-graceful shutdown by calling recover
let recovered_db_result = Dharma::<MyKey, MyValue>::recover(options);
```

## Features
* Store arbitrary key/value pairs.
* Data is sorted by key to ensure fast reads.
* Store Custom Data types using the Generic Interface.
* Sort order of data can be configured by implementing `Ord` trait
  for your data type.
* Fault Tolerant store with option for recovery in case of failure.

DharmaDB does not provide client-server communication. Applications can wrap
DharmaDB with a server to enable API access.

## Performance
Benchmarking is in a very nascent stage still.
Benchmarks have been added for `get` and `put` operations in the `benches` directory. Benchmarks were carried out on a macbook with the following configuration.

Results are summarized below.
```markdown
  Model Name:	MacBook Air
  Model Identifier:	MacBookAir10,1
  Chip:	Apple M1
  Total Number of Cores:	8 (4 performance and 4 efficiency)
  Memory:	8 GB
```

The performance numbers were gerated using the [Criterion.rs](https://github.com/bheisler/criterion.rs) package.

Performance was gauged by filling a database with a thousand initial
values and then flushing to disk to force sstable creation.
The benchmark scripts in `benches` folder were executed on this database.
```markdown
put operation time:   70.656 ms per operation
get operation time:   17.570 us per operation
```

## Contributing
Contributions to DharmaDB are welcome. For more complex PRs please
raise an issue outlining the problem / enhancement and how you intend to
solve it. All PRs should be accompanied with tests.
The docs folder contains an assortment of files that detail the
inner workings of DharmaDB.
The `tests` folder also reveals a lot about the inner workings of the database.
You can start off by reading **[DharmaDB Design](doc/dharmadb_design.md)**



