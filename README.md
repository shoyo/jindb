# <img src="images/logo.png" alt="Jin DB" width="450"/>
![Build Status](https://github.com/shoyo/jin/workflows/build/badge.svg)
[![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/shoyo/jin/blob/main/LICENSE)
[![codecov](https://codecov.io/gh/shoyo/jin/branch/main/graph/badge.svg)](https://codecov.io/gh/shoyo/jin)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/shoyo/jin)

## About
Jin is a small relational database engine written in [Rust](https://www.rust-lang.org) with the standard library and no external dependencies. It is currently being developed for 64-bit Linux, macOS, and Windows operating systems.

Although Jin is a prototype, it implements features of a fully-fledged database such as disk I/O for data persistence, a thread-safe buffer for in-memory caching, and (soon) logging mechanisms for crash recovery. It aspires to be a small, fast, relational database engine with ACID guarantees. 

## Current Progress
* Process management
    - [ ] Connection manager
* Query processor
    - [ ] SQL Parser
    - [ ] Query rewriter
    - [ ] Query optimizer
    - [ ] Query executor
* Transactions
    - [ ] Lock manager
    - [ ] Log manager
* Access methods
    - [ ] Hash indexes
    - [ ] B+ tree indexes
    - [ ] Btree indexes
* System catalog
    - [X] Create table
    - [ ] Alter table
    - [ ] Delete table 
    - [X] Read tuple 
    - [X] Insert tuple
    - [X] Update tuple
    - [X] Delete tuple
* Buffer pool
    - [X] Buffer manager (thread-safe in-memory cache)
    - [X] Page replacement manager (buffer page eviction)
* Persistence
    - [X] Disk manager (I/O for pages on disk)
* Data representation
    - [X] Table heaps
    - [X] Pages
    - [X] Tuples
    - [X] Schemas

## Development
Install the Rust toolchain [here](https://www.rust-lang.org/tools/install) or run:
```
% curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
Jin may require nightly Rust. Configure nightly with:
```
% rustup toolchain install nightly
% rustup override set nightly
```

To build the project during development, run:
```
% cargo build
```
Add `--release` to the end to create an optimized build.

To run all tests, run:
```
% cargo test
```

## Author
Shoyo Inokuchi (contact@shoyo.dev)
