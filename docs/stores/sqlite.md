# SQLite store

`SqliteTaskStore` keeps your tasks in one SQLite database file, for runners on one machine. This page shows how to set it up, what it needs, what it does, and what to watch for.

On this page:

- [Set up](#set-up)
- [Requirements](#requirements)
- [Settings on every connection](#settings-on-every-connection)
- [What the store does](#what-the-store-does)
- [Caveats](#caveats)
- [Inside the SQLite store](#inside-the-sqlite-store)

## Set up

Put SQLite in WAL mode with a busy timeout, then create the store from a pooled `DataSource` and a table prefix:

```java
var config = new SQLiteConfig();
config.setJournalMode(SQLiteConfig.JournalMode.WAL);
config.setBusyTimeout(5000);
config.setSynchronous(SQLiteConfig.SynchronousMode.FULL);
var source = new SQLiteDataSource(config);
source.setUrl("jdbc:sqlite:/var/lib/my-app/tasks.db");

var pool = new HikariConfig();
pool.setDataSource(source);
var store = new SqliteTaskStore(new HikariDataSource(pool), "bth");   // tables bth_task, bth_queue, bth_type, bth_worker, bth_schedule, bth_meta
```

The store is for one or more runners in one process, or in several processes on one machine, with the database file on a local disk.

Network file systems are not supported. SQLite's WAL mode does not work over them, and the store does not detect them. For runners on several machines use PostgreSQL or Redis.

The tables are created when the runner or client is built. Or run `store.schemaStatements()` from your migration tool. Runners with the same prefix on the same database file share the work.

## Requirements

| Item | Requirement |
|---|---|
| Module | `bth4j-sqlite`, which uses only `java.sql`. The application supplies the JDBC driver and a pooled `DataSource`. |
| SQLite | 3.45 or newer in WAL mode, the latest release recommended. The tests run on 3.54 (sqlite-jdbc 3.54.1.0, which carries its own SQLite). The store uses no feature newer than 3.45 and does not check the version. |
| Where it runs | One machine, with the database file on a local disk |
| `journal_mode` | `wal` on the connection `init` receives |
| `busy_timeout` | At least 1000 on the connection `init` receives |
| `synchronous` | Your choice. It is not checked. |
| Table prefix | Lower case letters, digits and underscores, starting with a letter, at most 41 characters, and neither `sqlite` nor starting with `sqlite_`, because SQLite reserves table names that start with `sqlite_` |
| Schema version | 2, stored in `<prefix>_meta`. `init` refuses tables with another version, which includes every database written by bth4j 4.2, and tables that have no `<prefix>_meta`; there is no migration. When the version matches, it only reads. |
| Clock | The store's `java.time.Clock`: the third constructor argument, `Clock.systemUTC()` by default |
| JDK 25 | The JVM prints a warning when sqlite-jdbc loads its native library, unless the application starts with `--enable-native-access=ALL-UNNAMED` |

## Settings on every connection

`init` refuses to run unless `PRAGMA journal_mode` is `wal` and `PRAGMA busy_timeout` is at least 1000 on the connection it receives. Its exception names the setting and how to set it.

| Setting | Must be | Without it |
|---|---|---|
| `journal_mode` | `wal` | a writer blocks readers |
| `busy_timeout` | at least 1000 | concurrent runners fail with `SQLITE_BUSY` instead of waiting for the write lock |

Set both on every connection, as in the example, or in the JDBC URL (`jdbc:sqlite:/path/tasks.db?journal_mode=WAL&busy_timeout=5000`).

`synchronous` is your choice and is not checked:

| Value | Effect |
|---|---|
| `FULL` | keeps every confirmed commit through a power cut |
| `NORMAL` | survives a crash of the application but can lose the last commits on power loss |

The [Stores](../stores.md#what-a-crash-can-lose) page lists what each setting loses, including `OFF`.

## What the store does

| Operation | What the store does |
|---|---|
| Clock | Takes times from the store's `java.time.Clock`, never from an SQL function. See [which clock decides](../stores.md#which-clock-decides). |
| Every write | Runs in a `BEGIN IMMEDIATE` transaction that the store issues itself, whatever the pool does to the connection. |
| Claim | Several short statements in one such transaction. It picks the ids for both passes of the queue grants, then leases them with one `UPDATE`. |
| A claim, promotion, purge or expiry that finds nothing to do | Reads first and takes no write lock. |
| Connections | The store returns every connection with the auto-commit setting it received. |
| `SqliteTaskStore.enqueue(Connection, TaskRecord)` | Stores a record on your connection and does not commit. The task exists only if your transaction commits. See [Enqueue in your own transaction](#enqueue-in-your-own-transaction). |
| A task removed inside its unique key's keep window | Leaves a key-only row in the task table until the window ends. The row is not counted or listed. |
| Foreign keys | None. |

## Caveats

Caveats that apply to every store, such as what a crash can lose, time precision and the order of tasks due at the same time, are on the [Stores](../stores.md) page.

### One writer at a time

SQLite allows one writer at a time. All runners on one SQLite database share a single write lock, so they write at the rate of one writer. Writes queue behind one another, so the write rate of the whole database is that of one writer.

A writer waits for the lock up to the busy timeout. A write that waits longer than `busy_timeout` fails with a `TaskStoreException`. The runner handles this like a short outage:

| Write that failed | What the runner does |
|---|---|
| A claim | It is logged, health records the store error, and the claim is tried again after `idlePollInterval` (see [Configuration](../configuration.md)). |
| An outcome | It is reported as `UNKNOWN` (see [Event outcomes](../monitoring.md#event-outcomes)). If it was not written, the lease expires and the task runs again. |

Keep your own write transactions on the database short.

### Pools that turn auto-commit off

A pool that turns auto-commit off together with `transaction_mode=IMMEDIATE` in the JDBC URL makes the driver begin a write transaction on every idle pooled connection. That holds the write lock. Do not combine the two.

### Enqueue in your own transaction

`SqliteTaskStore.enqueue(Connection, TaskRecord)` stores a record on your connection and does not commit. The task exists only if your transaction commits. See [Enqueue inside your own transaction](../stores.md#enqueue-inside-your-own-transaction) and [Enqueueing](../enqueueing.md#enqueue-inside-your-own-transaction).

| Situation | What to do |
|---|---|
| A write transaction holds SQLite's single write lock until it ends, and every other writer waits for it | Keep the transaction short. |
| A transaction reads first and writes later, and another writer committed in between | It can fail with `SQLITE_BUSY`. Begin it with `BEGIN IMMEDIATE`, or use `transaction_mode=IMMEDIATE` with a pool whose connections have auto-commit on. |
| Before your transaction begins | Call `client.prepare(request)`. It can allocate ids and register queue and type names, which writes on another connection. That write would wait for your transaction's write lock until the busy timeout. |
| `enqueue` throws a `TaskStoreException` | Roll your transaction back. SQLite undoes only the failed statement, so a commit would keep your other writes without the task. |

### A backlog of a type no runner handles

The queued partial index ends with `type_id`, and a claim walks the queued tasks of a queue in due order past those of types it does not handle. A large backlog of a type no live runner handles therefore makes every claim on that queue slower. Give such types their own queue.

## Inside the SQLite store

| Item | Detail |
|---|---|
| Tables | `<prefix>_task`, `<prefix>_queue`, `<prefix>_type`, `<prefix>_worker`, `<prefix>_schedule`, `<prefix>_meta` |
| Table mode | all tables are `STRICT` |
| Foreign keys | none |
| Times | epoch milliseconds, taken from the store's `java.time.Clock` |
| Schema version | 2, stored in `<prefix>_meta` |
| Claim lease | one `UPDATE` that takes the id list through `json_each` |
| Key-only row | state code 7, content columns empty |
| Queued partial index | ends with `type_id` |
