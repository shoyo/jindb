# Developer Documentation

## Overview
This document contains descriptions and rationale for the design and implementation of Jin. It 
mirrors the information on the [GitHub wiki](https://github.com/shoyo/jin/wiki/Developer-Documentation).

## Terminology
The internal code of Jin uses specific terms to refer to components of a database. Other 
databases may use different terms to refer to the same component.

* `Relation` refers to a table in the database.
* `Block` refers to the smallest unit of data that is stored on and fetched from disk. Its size 
  varies between databases, but is typically between 1KiB ~ 16KiB. The default size for Jin is 
  4KiB. Other databases may refer to blocks as pages.
* `Record` refers to a conceptual "row" in a relation. Records are stored together in blocks. 
  Records contained in a block always belong to the same table in Jin. Other databases may refer
  to records as tuples.

## Source Directory
The `src` directory contains the following.
* `block` - in-memory representations of disk blocks
* `buffer` - buffer pool management and block eviction policies
* `common` - common aliases and constants used throughout the codebase
* `concurrency` - lock and transaction management
* `disk` - disk management
* `execution` - plan nodes and executor definitions for executing queries
* `index` - database indexing
* `log` - log management and recovery
* `relation` - in-memory representations of relations
* `lib.rs` - main library 
* `main.rs` - main binary application

## Descriptions 

### Contents
1. [Buffer Manager](#buffer-manager)
2. [Relations](#relations)

### Buffer Manager
The buffer manager is responsible for fetching and flushing blocks that are held in an in-memory 
"buffer pool". Holding blocks in memory prevents frequent disk access and drastically speeds up
query execution. The managed "buffer pool" for a database serves a similar purpose to virtual
memory for RAM.

The buffer manager ...


### Relations
TODO