# Developer Documentation

## Overview
This document contains descriptions and rationale for the design and implementation of Jin. It 
mirrors the information on the [GitHub wiki](https://github.com/shoyo/jin/wiki).

*Warning*: This document is a work-in-progress and any information contained in it may be 
incomplete, inaccurate, and/or changed without advance notice.

## Terminology
The internal code of Jin uses specific terms to refer to components of a database. Other 
databases may use different terms to refer to the same component.

* `Page` refers to the smallest unit of data that is stored on and fetched from disk. Its size 
  varies between databases, but is typically between 1 KiB ~ 16 KiB. The default page size for 
  Jin is 4 KiB.
* `Record` refers to a conceptual "row" in a relation. Records are stored in pages. Records 
  contained in a page always belong to the same table in a row-oriented database like Jin. Other 
  databases may refer to records as tuples.
* `Relation` refers to a table in the database.
* `Heap` refers to a collection of pages that contain data for a relation. A heap-structured 
  database makes no guarantees about the ordering of pages in memory, meaning that records in a 
  relation are not necessarily laid out sequentially on disk.
* `Latch` refers to a mutex which protects critical sections of in-memory data structures from
  separate threads. Operating systems refer to latches as locks, but locks typically refer to a 
  different concept in the context of database concurrency.
* `Lock` refers to the access control mechanism for transactions in the database. They protect the 
  data contained in a database so that separate user transactions don't mutate the data in a way 
  that violates ACID principles.
  
In other words,`Relations` are organized as `Heaps` which are a collection of `Pages` which all 
contain `Records`. Critical sections of internal data structures are protected from separate threads by 
`Latches`, and stored data is protected from separate transactions by `Locks`.

## Source Directory
The `src` directory contains the following modules.
* `page` - in-memory representations of disk pages
* `buffer` - buffer pool management and page eviction policies
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
flushing pages that are held in an in-memory buffer pool. Holding pages in memory reduces the 
need for disk access and drastically speeds up query execution. The managed buffer pool for a
database serves a similar purpose to virtual memory for RAM.

The buffer pool is internally represented as a vector of buffer frames. A buffer frame may or 
may not contain a page fetched from disk, so is represented as an `Option<Page>`. Threads may 
access the buffer pool vector without acquiring a latch (i.e. the vector itself is not wrapped 
in any `Arc` or `Mutex`). However, in order for a thread to index the buffer pool and access a 
specific buffer frame, a reader-writer latch must first be acquired. Once the latch is acquired, 
a thread can access the `Option<Page>`. Threads will page until they acquire the latch they
request.

Any code interacting with pages must be thread-safe. To satisfy this, any buffer manager API 
that returns a reference to a page returns a page latch, or a page guarded by a reader-writer
lock defined as `Arc<RwLock<Option<Page>>>`. A reader-writer lock allows either a single writer 
or multiple readers to access the underlying data at any given time.

Higher layers of the database make queries to the buffer manager whenever they want to access a 
given page. If the requested page already exists in the buffer, the page's latch is returned
to the caller immediately. Otherwise, the buffer manager makes a request to the disk manager to
retrieve the page from disk.


### Relations and Heaps
Relations are represented by one or more pages on disk which are connected as a doubly linked 
list. Each page is assigned a unique ID by the disk manager when it is initialized. Each page 
stores in its header metadata about its contents, including the ID of "next" and "previous"
pages in its sequence.

### Disk manager
The disk manager ...
...the disk manager is inherently thread-safe so doesn't require a mutex.