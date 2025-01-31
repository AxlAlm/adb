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

- event sourcing db allowing to append and read event from streams
- simple cli for db interaction
- queries and mutations in datalog

### limitation

- only in memory

### future extensions

- data backups on disk (WAL file(?))
- reads/writes to/from disk
- generated code for type safe and easy interaction with
- live projections and read models
