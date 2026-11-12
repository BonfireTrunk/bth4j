# PostgreSQL store

`PostgresTaskStore` keeps your tasks in PostgreSQL tables, so runners on several machines can share the work. This page shows how to set it up, what it needs, what it does, and what to watch for.

On this page:

- [Set up](#set-up)
- [Requirements](#requirements)
- [What the store does](#what-the-store-does)
- [Caveats](#caveats)
- [Inside the PostgreSQL store](#inside-the-postgresql-store)

## Set up

Create the store from a pooled `DataSource` and a table prefix:

```java
var store = new PostgresTaskStore(dataSource, "bth");   // tables bth_task, bth_queue, bth_type, bth_worker, bth_schedule, bth_meta
```

The tables are created when the runner or client is built. Or run `store.schemaStatements()` from your migration tool.

Runners with the same prefix on the same database share the work.

## Requirements

| Item | Requirement |
|---|---|
| Module | `bth4j-postgres`, which uses only `java.sql`. The application supplies the JDBC driver and a pooled `DataSource`. |
| PostgreSQL | 16 or newer is supported, 17 or newer recommended. The tests run on 18. |
| Isolation level | The store runs at the connection's isolation level. It is written for READ COMMITTED, PostgreSQL's default. |
| Table prefix | Lower case letters, digits and underscores, starting with a letter, at most 41 characters |
| Schema version | 4, stored in `<prefix>_meta`. `init` refuses tables with another version, which includes every table set written by bth4j 4.x or older; there is no migration. Start with a new prefix or drop the tables. |
| Privileges of a runner role on existing tables | SELECT, INSERT, UPDATE and DELETE on the tables, and USAGE on the sequences |
| Query timeouts | None set by the store. Configure them on the driver and pool (see [No query timeout](#no-query-timeout)). |

## What the store does

| Operation | What the store does |
|---|---|
| Clock | Takes the current time from the database server: the time the statement started (`statement_timestamp()`), cut to whole milliseconds. Inside your own transaction that is the time of each statement, not the start of the transaction. See [which clock decides](../stores.md#which-clock-decides). |
| `init` | Takes a transaction-level advisory lock for the prefix. Then it only reads the stored schema version when the tables exist. It runs DDL only when there is no schema yet. |
| Starting a runner | Takes no table lock. |
| Claim | One statement that runs both grant passes, each with a bounded per-queue `FOR UPDATE SKIP LOCKED` scan, so a claim is atomic without a transaction. |
| `queuedByType()` | One `GROUP BY` over the queued rows. |
| `counts()` | Reads an index of the counts per queue and state. |
| Listing one finished state of one queue | Reads an index of the finished tasks of that queue. |
| Listing QUEUED across all queues | Scans the queued index once per queue and merges by due time and id. |
| Foreign keys | None. |
| Enqueue in your own transaction | `PostgresTaskStore.enqueue(Connection, TaskRecord)` stores a record on your connection and does not commit. See [Enqueue inside your own transaction](../stores.md#enqueue-inside-your-own-transaction) and [Enqueueing](../enqueueing.md#enqueue-inside-your-own-transaction). |
| A task removed inside its unique key's keep window | Leaves a key-only row in the task table until the window ends. The row is not counted or listed. |

## Caveats

Caveats that apply to every store, such as what a crash can lose, time precision and the order of tasks due at the same time, are on the [Stores](../stores.md) page.

### Isolation level

The store is written for READ COMMITTED, PostgreSQL's default. Under REPEATABLE READ or SERIALIZABLE, concurrent claims and fenced writes fail with serialization errors.

### No query timeout

The store sets no query timeout of its own. Configure them on the driver and pool:

| Setting | Notes |
|---|---|
| `socketTimeout` | for pgjdbc |
| `tcpKeepAlive` | for pgjdbc |
| `statement_timeout` | for example `options=-c statement_timeout=30s` |

Without them, a half-open connection can block a runner thread for as long as the operating system keeps the socket, while health still reports RUNNING.

A write whose reply was lost to a timeout may or may not have committed. The runner treats it as `UNKNOWN` (see [Event outcomes](../monitoring.md#event-outcomes)).

### A backlog of a type no runner handles

A claim walks the queued tasks of a queue in due order past those of types it does not handle. A large backlog of a type no live runner handles therefore makes every claim on that queue slower. Give such types their own queue.

## Inside the PostgreSQL store

### Schema

| Item | Detail |
|---|---|
| Tables | `<prefix>_task`, `<prefix>_queue`, `<prefix>_type`, `<prefix>_worker`, `<prefix>_schedule`, `<prefix>_meta` |
| Foreign keys | none |
| Schema version | 4 |
| Times | `timestamptz(3)` columns: whole milliseconds |
| Task types | stored in `<prefix>_type`, and as `type_id` columns in the task and schedule tables |
| Runner coverage | `queue_ids` and `type_ids` as `smallint[] NOT NULL` |
| `accepting` | a required boolean, true on registration and updated by the heartbeat |
| `payload_version`, `catch_up_max` | `integer`, so every positive Java `int` is stored |
| Key-only row | state code 7, content columns empty |

### Indexes and reads

| Operation | What it reads |
|---|---|
| Claim | the queued tasks of a queue in due order, past those of types it does not handle |
| `queuedByType()` | one `GROUP BY queue_id, type_id` over the queued rows |
| `counts()` | the `<prefix>_task_counts` index (`queue_id, state`) |
| Listing one finished state of one queue | `<prefix>_task_finished_queue` |
| Listing QUEUED across all queues | the queued index, once per queue, merged by due time and id |

The queued partial index ends with `type_id` to cover the `queuedByType()` counts while preserving claim order. It uses ordinary index columns.
