# Background Task Handler (BTH4J)

This Java library provides a robust and flexible framework for managing background tasks. It leverages Redis for persistent task storage and queue management, ensuring reliability and scalability.

## Overview

BTH4J is designed to handle various types of tasks:

* **Regular Tasks:** Tasks executed once and then removed from the queue.
* **Scheduled Tasks:** Tasks scheduled to run at a specific future time.
* **Recurring Tasks:** Tasks executed periodically at defined intervals.

Tasks are claimed in batches with a lease, retried with a configurable backoff, and every state change is one atomic transition in the store, so several instances can share one store safely. The store is an interface with two implementations, Redis (`bth4j-redis`) and PostgreSQL (`bth4j-postgres`). Callbacks for task lifecycle events (before start, success, error, after task) are supported.

## Installation

Add the store module you use; the core library comes with it:

```xml
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-redis</artifactId>       <!-- or bth4j-postgres -->
    <version>3.0.0</version>
</dependency>
```

Requires Java 21. The Redis store needs Redis 7.0 or newer and also works on Redis Cluster (all keys of a store share
one hash slot). The PostgreSQL store needs PostgreSQL 14 or newer and the application's own JDBC driver and
`DataSource`.

## How it works

A task is always in one of four states: queued, scheduled, leased or dead. Each state change is one atomic
call on the `TaskStore` that checks the task's lease token, so a task runs again only when the worker holding
it died or could not renew its lease. Several runners on the same store share the work without any locks.
The Redis store does each transition in one Lua script, called by hash; the PostgreSQL store does it in one SQL
statement with `FOR UPDATE SKIP LOCKED` claims. The design, the store contract and both layouts are described in
[docs/design-3.0.md](docs/design-3.0.md).

```
enqueue  --(no due)-->  queue  --claim-->  leased  --ack-->  gone
   |                      ^                  |  |
   +--(due)--> scheduled --+                 |  +--fail, attempts left--> scheduled (retry delay)
                  ^                          |  +--fail, exhausted-----> dead (kept 30 days)
                  +--------reschedule--------+  +--lease expired------> retried by maintenance
```

Components:

- **BackgroundRunner** starts and supervises the services and shuts them down in order.
- **TaskProcessorService** reserves executor slots, claims that many tasks in one call, hands them to the
  executor and renews their leases while they run. It is woken by a Redis channel when a task is enqueued.
- **ScheduledTaskService** moves due tasks from the scheduled set into their queues and sleeps until the next due time.
- **RecurringTaskService** creates the next occurrence of each recurring definition ahead of its time.
- **MaintenanceService** retries tasks whose lease expired and purges old dead tasks.
- **TaskOps** is the application's entry point: enqueue, recurring definitions, inspection. It validates queues,
  serializes data and delegates every transition to the store.
- **TaskStore** is the interface a store implements (`bth4j-core`); **RedisTaskStore** (`bth4j-redis`) and
  **PostgresTaskStore** (`bth4j-postgres`) are the implementations.
- **BackgroundExecutor** bounds concurrency; `DefaultVtExecutor` (virtual threads, adaptive limit) and
  `DefaultPtExecutor` (platform threads) are provided.

## Usage

### 1. Events

Events identify task types and carry the retry policy. An enum is the usual choice:

```java
public enum AppEvent implements Event {
    SEND_MAIL(false, 3), NIGHTLY_REPORT(true, 0), UNKNOWN(false, 0);

    private final boolean recurring;
    private final int retries;

    AppEvent(boolean recurring, int retries) { this.recurring = recurring; this.retries = retries; }

    @Override public boolean isRecurring() { return recurring; }
    @Override public int retryCount() { return retries; }
    @Override public String value() { return name(); }
    @Override public Event from(String value) {
        try { return valueOf(value); } catch (IllegalArgumentException e) { return UNKNOWN; }
    }
}
```

A task is attempted `retryCount() + 1` times before it is moved to the dead set.

### 2. Task processors

```java
public class SendMail implements TaskProcessor<MailRequest> {
    @Override
    public void process(Task task, MailRequest data) {
        // task.attempts() tells which attempt this is
    }

    @Override
    public Class<MailRequest> dataTypeClass() {
        return MailRequest.class;
    }
}

public class NightlyReport implements TaskProcessor<Void> {
    @Override
    public void process(Task task, Void data) { ... }
}
```

Processors must be idempotent: delivery is at least once.

Throw `TaskUnrecoverableException` to move a task to the dead set at once, `TaskRescheduleException(delaySeconds)`
to run the same task again later without using an attempt, or any other exception to fail the attempt and retry
after the configured delay.

### 3. Runner

```java
var jedis = RedisClient.builder()
    .hostAndPort(new HostAndPort("localhost", 6379))
    .clientConfig(DefaultJedisClientConfig.builder().resp3().clientName("my-app").build())
    .poolConfig(ConnectionPoolConfig.builder().maxPoolSize(32).build())
    .build();

var registry = new TaskProcessorRegistry();
registry.register(AppEvent.SEND_MAIL, new SendMail());
registry.register(AppEvent.NIGHTLY_REPORT, new NightlyReport());

var runner = new BackgroundRunner.Builder()
    .taskStore(new RedisTaskStore(jedis, "my-app"))   // runners with the same namespace share the work
    .eventParser(AppEvent.UNKNOWN::from)
    .taskProcessorRegistry(registry)
    .jsonMapper(myJsonMapper)              // needed when tasks carry data
    .configureQueues(List.of("default", "mail", "reports"), List.of("default", "mail", "reports"), "default")
    .leaseDuration(Duration.ofSeconds(60))
    .build();

runner.run();                              // returns at once, the services run on their own threads
...
runner.stopRunner();                       // waits for running tasks, then
jedis.close();
```

### 4. Enqueueing tasks

```java
var taskOps = runner.taskOps();

taskOps.enqueue(Task.Builder.newTask()
        .accountId(customerId)
        .event(AppEvent.SEND_MAIL)
        .queueName("mail")
        .build(),
    new MailRequest(...));

taskOps.enqueue(Task.Builder.newTask("order-42-reminder")   // your own id, duplicates are rejected
        .accountId(customerId)
        .event(AppEvent.SEND_MAIL)
        .executeAfter(Duration.ofHours(24))
        .build(),
    new MailRequest(...));

taskOps.addRecurringTask(Task.Builder.newTask("nightly-report")
        .accountId("system")
        .event(AppEvent.NIGHTLY_REPORT)
        .cronExpression("30 2 * * *")       // 5 field unix cron, UTC
        .queueName("reports")
        .build());
```

`enqueue` returns `false` when a task with that id already exists. Recurring definitions are keyed by the task id;
registering the same id again updates the definition and keeps the next fire time unless the cron expression changed.

### 5. Callbacks

```java
new BackgroundRunner.Builder()
    .taskHandlerDecorators(
        task -> log.info("starting {}", task),
        task -> log.info("done {}", task),
        (task, ex) -> log.warn("failed {} on attempt {}", task, task.attempts(), ex),
        task -> metrics.record(task))
    .recurringTaskHandlerDecorators(...)
```

Callbacks run after the outcome has been applied to Redis and never change it.

### 6. Inspection

```java
taskOps.queueSize("mail");
taskOps.leasedCount();              // running now, across all instances
taskOps.scheduledCount();
taskOps.deadTasks(100);             // with lastError() and attempts()
taskOps.retryDeadTask(id);
taskOps.getTask(id);                // any state
```

## Configuration

| Builder setting | Default | Meaning |
|---|---|---|
| `taskStore` | required | the store: `new RedisTaskStore(jedis, "namespace")` or `new PostgresTaskStore(dataSource, "prefix")` |
| `leaseDuration` | 60 s | how long a claimed task stays leased without renewal; renewed every third while running |
| `taskRetryDelay` | exponential, 1 s doubling, capped at 1 h, 20 % jitter | delay before a failed task is retried, by attempt number |
| `idlePollInterval` | 1 s | fallback wait when the queues are empty or the executor is full |
| `claimBatchSize` | 32 | most tasks claimed in one Redis call |
| `maintenanceCheckInterval` | 5 s | how often expired leases are retried |
| `scheduledTasksCheckInterval` | 1 s | upper bound of the scheduler's wait; it wakes earlier for the next due task |
| `recurringTasksCheckInterval` | 10 s | how often recurring definitions are checked |
| `recurringTasksQueueAheadDuration` | 1 h | how far ahead occurrences are created in the scheduled set |
| `deadTaskRetention` | 30 d | how long dead tasks are kept |
| `shutdownTimeout` | 30 s | how long `stopRunner()` waits for services and running tasks |
| `serviceCheckInterval` | 10 s | how often dead service threads are restarted |
| `configureQueues` | `Q:DEF`, `Q:LOW`, `Q:MED`, `Q:HIGH` | available queues, queues to process in round robin, default queue |

## PostgreSQL

```java
var store = new PostgresTaskStore(dataSource, "bth");   // tables bth_task, bth_schedule; runners sharing them share the work
store.createSchema();                                   // once, or run store.schemaStatements() from your migration tool
var runner = new BackgroundRunner.Builder().taskStore(store) ...
```

The store uses only `javax.sql.DataSource` plus the driver's `LISTEN/NOTIFY` API, so the application supplies the
`org.postgresql` driver and a pooled data source. Every transition is one SQL statement: claims take rows with
`FOR UPDATE SKIP LOCKED`, the token-checked transitions are `UPDATE ... WHERE id = ? AND token = ?`, and a trigger
sends `NOTIFY` whenever a task becomes runnable. The wake-up subscription holds one pooled connection while the
runner runs; size the pool accordingly. The same integration tests run against both stores; in them PostgreSQL
processes about half as many tasks per second as Redis.

## Other stores

`TaskStore` (`today.bonfire.oss.bth4j.store`) is the whole contract between the runner and the storage: enqueue,
claim, and token-checked transitions, each of which the store must make atomic. A store on another engine
implements that one interface and is passed to `taskStore(...)`; nothing else in the application changes. The
`bth4j-store-tests` module holds the contract tests every store must pass. See "Writing a store" in
[docs/design-3.0.md](docs/design-3.0.md).

## Upgrading from 2.x

3.0 cannot read 2.x data. Let the 2.x runners drain their queues, then start 3.0, or use a new namespace.
The artifact is now `bth4j-redis` or `bth4j-postgres` (each brings `bth4j-core`), and the runner takes a
`TaskStore` instead of a Jedis client and a namespace prefix. API changes are listed in `CHANGELOG.md`.

## Limitations

- Cron expressions use the 5 field Unix format and are evaluated in UTC.
- The Redis store needs Redis 7.0 or newer (or a compatible server with `EVAL`, `LMPOP` and `ZRANGE BYSCORE`).
- The PostgreSQL store needs PostgreSQL 14 or newer (`CREATE OR REPLACE TRIGGER`) and `SKIP LOCKED`.
- Java 21 or later is required.

## Contributing

- Build and test with the Maven Wrapper: `./mvnw verify`. Java 21 or later is required.
- The build has the modules `bth4j-core`, `bth4j-redis` and `bth4j-postgres`, released together under one version, plus `bth4j-store-tests` (the contract tests, not published). Unit tests run in the `test` phase. The integration tests (`*IT` in the store modules) start a Redis or PostgreSQL container with Testcontainers in the `integration-test` phase and need Docker.
- Versioning: the first change after a release bumps the version in `pom.xml` to the next `-SNAPSHOT`. A release is a pull request titled `Release X.Y.Z` that sets the final version and adds a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md`; merging it publishes the release.
