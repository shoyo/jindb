# Developer Documentation

## Overview
This document contains descriptions and rationale for the design and implementation of Jin. It 
mirrors the information on the [GitHub wiki](https://github.com/shoyo/jin/wiki/Developer-Documentation).

*Warning*: This document is a work-in-progress and any information contained in it may be 
incomplete, inaccurate, and/or changed without advance notice.

## Terminology
The internal code of Jin uses specific terms to refer to components of a database. Other 
databases may use different terms to refer to the same component.

* `Relation` refers to a table in the database.
* `Block` refers to the smallest unit of data that is stored on and fetched from disk. Its size 
  varies between databases, but is typically between 1 KiB ~ 16 KiB. The default block size for 
  Jin is 4 KiB. Other databases may refer to blocks as pages.
* `Record` refers to a conceptual "row" in a relation. Records are stored in blocks. Records 
  contained in a block always belong to the same table in a row-oriented database like Jin. Other 
  databases may refer to records as tuples.
* `Latch` refers to a mutex which protects critical sections of in-memory data structures from
  separate threads. Operating systems refer to latches as locks, but locks typically refer to a 
  different concept in the context of database concurrency.
* `Lock` refers to the access control mechanism for transactions in the database. They protect the 
  data contained in a database so that separate user transactions don't mutate the data in a way 
  that violates ACID principles.

## Source Directory
The `src` directory contains the following modules.
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

## Details 

### Contents
1. [Database buffer](#database-buffer)
2. [Relations and Heaps](#relations-and-heaps)

### Database buffer
Jin, like other disk-oriented databases, implements a database buffer. The 
database buffer is managed by the buffer manager, which is responsible for fetching and
flushing blocks that are held in an in-memory buffer pool. Holding blocks in memory reduces
disk access and drastically speeds up query execution. The managed "buffer pool" for a
database serves a similar purpose to virtual memory for RAM.

The buffer pool is internally represented as a vector of buffer frames. A buffer frame may or 
may not contain a block fetched from disk, so are represented as an `Option<Block>`. Threads may 
access the buffer pool vector without acquiring a latch (i.e. the vector itself is not wrapped 
in any `Arc` or `Mutex`). However, in order for a thread to index the buffer pool and access a 
specific buffer frame, a reader-writer latch must first be acquired. Once the latch is acquired, 
a thread can access the `Option<Block>`. Threads will block until they acquire the latch they
request.

Any code interacting with blocks must be thread-safe. To satisfy this, any buffer manager API 
that returns a reference to a block returns a block latch, or a block guarded by a reader-writer
lock defined as `Arc<RwLock<Option<Block>>>`. A reader-writer lock allows either a single writer 
or multiple readers to access the underlying data at any given time.

Higher layers of the database make queries to the buffer manager whenever they want to access a 
given block. If the requested block already exists in the buffer, the block's latch is returned
to the caller immediately. Otherwise, the buffer manager makes a request to the disk manager to
retrieve the block from disk.


### Relations and Heaps
Relations are represented by one or more blocks on disk which are connected as a doubly linked 
list. Each block is assigned a unique ID by the disk manager when it is initialized. Each block 
stores in its header metadata about its contents, including the ID of "next" and "previous"
blocks in its sequence.
