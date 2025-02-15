# aDB

Event Sourcing DB written in rust

## motivation

Motivation for this project is to deal with the two following problems with event sourcing

### 1. Event Sourcing is hard to implement

- Versioning
- Defining events
- Keeping
- reading streams
- etc

### 2. Its hard to interact with Event Sourcing systems "ad hod" demands

Getting "ad hoc" request about extracting some data is commonplace when maintaining a system. More seldom, but not non-existent, are request or the internal need for mutations.

Working with a relation database these requests or needs are possible to meet wihtout too much hassle; most cases its a simple SQL query.

Working with event sourcing systems this is not as easy. Often in would entail deploying new code to support the need. And in cases where you do have access to query and mutate directly in the database, e.g. in cases where you use a relation database to store your events, those queries would not be simple!

**In ADB these issues are addressed by allowing relational queries and mutations using datalog**

## Features

- schema
- mutations (add)
- queries
- transaction
- language for defining schema, mutation, queries and transactions
- cli to interact with db

### future extensions

- reads/writes to/from disk (i.e. not just in memory)
- backups (WAL file(?))
- generated code for type safe and easy interaction with
- live projections and read models

### Write Concurrency

Will be opting for Optimistic Concurrency. This means that we can concurrently write to multiple streams or within a stream if the keys for events differ.
However, to ensure serialization of events writes to stream keys are serial. This will be acheived by doing the following;

1. on writing a write lock will be aquired
2. version of last added event will be fetched
3. a check will be done to ensure that the version of the event that is being added is equal to last added events version + 1
4. if (3) fails transaction will fail, if (3) passed event will be written
5. write lock is released

#### TODO

- [] migrate (naive)
- [] data storing (in memory)
- [] mutation operation (validation + transaction + add to event stream)
- [] simple cli
- [] query language and parser for queries
- [] query planning and execution
- [] data indexing to optimize queries
