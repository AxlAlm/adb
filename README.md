# aDB

Event Sourcing DB written in rust (currently only in memory)

## Features

- operations for defining schema, adding events and querying data
- simple DSL for interacting with database
- TCP for server-client communication
- simple CLI to interact with database

### future extensions

- security (credentials)
- transactions
- reads/writes to/from disk (i.e. not just in memory)
- backups (WAL file?)
- (rust client lib)
- (live projections and read models)

## Operations

### Create

The `create` operation allows you expand the schema by creating new streams, events on streams and attributes on events.

To create a new stream in the schema the syntax is as follows:

    create stream(<STREAM NAME>);

example

    create stream(account);

To create a new event that can be added to a stream the syntax is as follows:

    create event(<STREAM NAME>, <EVENT NAME>);

example

    create event(account, AccountCreated);

To create attributes on an event the syntax is as follows;

    create attribute(<STREAM NAME>, <EVENT NAME>, <ATTRIBUTE NAME>, <IF REQUIRED>, <TYPE OF THE ATTRIBTE>);

example

    create (account, AccountCreated, owner-name, true, string);

### Show

The `show` operation allows you to show the schema. To do this you run;

    show schema;

### Add

The `add` operation allows you to add events to a stream. Syntax for adding an event is:

    add <EVENT NAME>(<ATTRIBUTE NAME>=<ATTRIBUTE VALUE> ... ) -> <STREAM NAME>:<KEY>;

`<KEY>` is the id of the stream, e.g. a user or account or some domain thingy.

Example:

    add AccountCreated(owner-name="axel") -> account:123;

### Find

The `find` command lets you query the database. It supports relational queries and aggregation.

TBA

## motivation

Motivation for this project is to deal with the two following problems with event sourcing

### 1. Event Sourcing is hard to implement

While the real challange with Event Sourcing lies in domain modelling creating the "foundation" needed is not always easy.

Adb provides this needed foundation so focus could be placed on domain modelling.

### 2. Its hard to interact with Event Sourcing systems "ad hod" demands

Getting "ad hoc" request about extracting some data is commonplace when maintaining a system. More seldom, but not non-existent, are request or the internal need for mutations.

Working with a relation database these requests or needs are possible to meet without too much hassle; in most cases its a "simple" SQL command.

Working with event sourcing systems this is not as easy. Often in would entail deploying new code to support the need. And in cases where you do have access to query and mutate directly in the database, e.g. in cases where you use a relation database to store your events, those queries would not be simple!

Adb solves this by providing a simple way to interact with the database; allowing relation queries, quick and simple ways to add events or define new ones.

## Concurrency

## Reading

Reading is non-blocking and always concurrent

## Writing

Writing only blocks writes by stream key. I.e. one can write concurrent to two different streams or two different stream keys, but not to the same stream key.
This is because it is crucial that events are sertially ordered. Hence, writes to a specific stream key will block any other writes to that stream key.

Blocking writes for stream keys is not by itself enough to ensure "Serializability"(?). This is ensured by doing the following;

1. on writing a write lock will be aquired
2. version of last added event will be fetched
3. a check will be done to ensure that the version of the event that is being added is equal to last added events version + 1
4. if (3) fails the write will fail, if (3) passed event will be written
5. write lock is released

#### TODO

- [] query language and parser for queries
- [] query planning and execution
- [] data indexing to optimize queries
