# <img src="images/logo.png" alt="Jin DB" width="450"/>
![Build Status](https://github.com/shoyo/jin/workflows/build/badge.svg)
[![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/shoyo/jin/blob/main/LICENSE)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/shoyo/jin)

## About
Jin is a small relational database engine written in [Rust](https://www.rust-lang.org) with the standard library and no external dependencies. It is currently being developed for 64-bit Linux and macOS operating systems.

Although Jin is a prototype, it implements features of a fully-fledged database such as a disk I/O for data persistence, a thread-safe buffer for in-memory caching, and (soon) logging mechanisms for crash recovery. It aspires to be a small, fast, relational database engine with ACID guarantees. 

## Development
Install the Rust toolchain [here](https://www.rust-lang.org/tools/install) or run:
```
% curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
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
