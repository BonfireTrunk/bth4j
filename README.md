# Background Task Handler (BTH4J)

This Java library provides a robust and flexible framework for managing background tasks. It leverages Redis for persistent task storage and queue management, ensuring reliability and scalability.

## Overview

BTH4J is designed to handle various types of tasks:

* **Regular Tasks:** Tasks executed once and then removed from the queue.
* **Scheduled Tasks:** Tasks scheduled to run at a specific future time.
* **Recurring Tasks:** Tasks executed periodically at defined intervals.

Tasks are claimed in batches with a lease, retried with a configurable backoff, and every state change is an atomic Redis script, so several instances can share one namespace safely. Callbacks for task lifecycle events (before start, success, error, after task) are supported.

## Installation

Add the following dependency to your project:

```xml
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j</artifactId>
    <version>3.0.0</version>
</dependency>
```

Requires Java 21 and Redis 7.0 or newer (also works on Redis Cluster: all keys of a runner share one hash slot).

## How it works

Every task is a Redis hash; queues hold task ids. Each state change of a task is one atomic Lua script that
checks the task's lease token, so a task runs again only when the worker holding it died or could not renew
its lease. Several runners with the same namespace share the work without any locks. The design, the key
layout and the scripts are described in [docs/design-3.0.md](docs/design-3.0.md).

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
- **TaskOps** is the Redis store: enqueue, recurring definitions, inspection, and the transitions above.
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
    .jedisClient(jedis)
    .eventParser(AppEvent.UNKNOWN::from)
    .taskProcessorRegistry(registry)
    .jsonMapper(myJsonMapper)              // needed when tasks carry data
    .namespacePrefix("my-app")
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
| `namespacePrefix` | `BTH` | runners with the same namespace share the work |
| `configureQueues` | `Q:DEF`, `Q:LOW`, `Q:MED`, `Q:HIGH` | available queues, queues to process in round robin, default queue |

## Upgrading from 2.x

3.0 cannot read 2.x data. Let the 2.x runners drain their queues, then start 3.0, or use a new namespace.
API changes are listed in `CHANGELOG.md`.

## Limitations

- Cron expressions use the 5 field Unix format and are evaluated in UTC.
- Redis 7.0 or newer (or a compatible server) is required.
- Java 21 or later is required.

## Contributing

- Build and test with the Maven Wrapper: `./mvnw verify`. Java 21 or later is required.
- Unit tests run in the `test` phase. The Redis integration tests (`*IT`) start a Redis container with Testcontainers in the `integration-test` phase and need Docker.
- Versioning: the first change after a release bumps the version in `pom.xml` to the next `-SNAPSHOT`. A release is a pull request titled `Release X.Y.Z` that sets the final version and adds a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md`; merging it publishes the release.
