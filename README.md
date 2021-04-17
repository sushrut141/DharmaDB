# DharmaDB

DharmaDB is a persistent, fault tolerant Key-Value Store written in Rust.


![Build](https://github.com/sushrut141/dharma/workflows/Build/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)

## Setup
Just create an instance of `Dharma` to get the key value store up and running.
```rust
use dharma::dharma::Dharma;
use dharma::errors::Errors;
use dharma::options::DharmaOpts;

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
use dharma::dharma::Dharma;
use dharma::errors::Errors;
use dharma::options::DharmaOpts;

let options = DharmaOpts::default();
let db_result: Result<Dharma<MyKey, MyValue>, Errors> = Dharma::new(options);

// persist key / value pair
let put_result = db.put(my_key, my_value);
```

### get

The get operation retrieves the value associated with a key if it exists.
```rust
use dharma::dharma::Dharma;
use dharma::errors::Errors;
use dharma::options::DharmaOpts;

let options = DharmaOpts::default();
let db_result: Result<Dharma<MyKey, MyValue>, Errors> = Dharma::new(options);

// get the key if it exists
let get_result = db.get(&my_key);
let maybe_value: Option<MyValue> = get_result.unwrap();
```

### recover
The recover operation is required in cases of unexpected crashes.
Generally, `Dharma` will detect non-graceful exit and suggest running
recovery on startup.
```rust
use dharma::dharma::Dharma;
use dharma::errors::Errors;
use dharma::options::DharmaOpts;

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

## Contributing
Contributions to DharmaDB are welcome. For more complex PRs please
raise an issue outlining the problem / enhancement and how you intend to
solve it. The docs folder contains an assortment of files that detail the 
inner workings of DharmaDB.

You can start off by reading **[DharmaDB Design](doc/dharmadb_design.md)**



