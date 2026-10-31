# Background Task Handler (BTH4J)

A Java library for background tasks with at-least-once delivery. Tasks live in a store (Redis or
PostgreSQL), several instances share the work through that store, and every state change is one atomic
transition guarded by a revision, so a worker that lost its task cannot change it.

## Overview

BTH4J handles three kinds of work:

* **Immediate tasks:** enqueued and run as soon as a worker is free.
* **Scheduled tasks:** enqueued with a delay or a run time.
* **Recurring tasks:** a cron definition that creates one task per fire time.

A task has a type (`TaskType`) with a payload, a queue, an optional unique key, a retry budget and, while
running, a lease that the runner renews. A processor can split its work into durable steps so that a retry
resumes where the failed attempt stopped. The store is an interface with a Redis, a PostgreSQL and an
in-memory implementation.

## Installation

Add `bth4j-core` and one store module. Add `bth4j-testing` for tests.

```xml
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-core</artifactId>
    <version>3.0.0</version>
</dependency>
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-redis</artifactId>       <!-- or bth4j-postgres -->
    <version>3.0.0</version>
</dependency>
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-testing</artifactId>
    <version>3.0.0</version>
    <scope>test</scope>
</dependency>
```

| Module | Content |
|---|---|
| `bth4j-core` | task model, runner, services, the store interface (`today.bonfire.oss.bth4j.spi`) |
| `bth4j-redis` | `RedisTaskStore`: Redis 7.0 or newer, also on Redis Cluster; uses Jedis |
| `bth4j-postgres` | `PostgresTaskStore`: PostgreSQL 9.5 or newer; uses only `java.sql`, the application supplies the JDBC driver and a pooled `DataSource` |
| `bth4j-testing` | `InMemoryStore` and `MutableClock` for tests of applications |
| `bth4j-store-tests` | `StoreContract`, the tests every store must pass (not published) |

Requires Java 21.

## Quick start

A payload is converted to text by a `JsonMapper`, a four-method interface that wraps the JSON library you
already use. With Jackson (2.12 or newer for records):

```java
public class JacksonMapper implements JsonMapper {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override public <T> T fromJson(String json, Class<T> type) throws Exception { return mapper.readValue(json, type); }
    @Override public <T> T fromJson(byte[] json, Class<T> type) throws Exception { return mapper.readValue(json, type); }
    @Override public String toJson(Object o) throws Exception { return mapper.writeValueAsString(o); }
    @Override public byte[] toJsonAsBytes(Object o) throws Exception { return mapper.writeValueAsBytes(o); }
}
```

Declare one `TaskType` constant per kind of task and use it for enqueueing and for the processor:

```java
public record MailRequest(String to, String subject) {}

static final TaskType<MailRequest> SEND_MAIL = TaskType.of("send-mail", MailRequest.class);
static final TaskType<Void> NIGHTLY_REPORT = TaskType.of("nightly-report");
```

Build a runner on a store, register the processors, start it and enqueue through its client:

```java
var jedis = RedisClient.builder()
        .hostAndPort(new HostAndPort("localhost", 6379))
        .build();

var runner = BackgroundRunner.builder()
        .store(new RedisTaskStore(jedis, "my-app"))   // runners with the same namespace share the work
        .jsonMapper(new JacksonMapper())
        .build();

runner.registry()
        .register(SEND_MAIL, (ctx, mail) -> {
            // split the work into steps: a retry skips the steps that completed
            String body = ctx.step("render", String.class, () -> render(mail));
            ctx.step("deliver", () -> deliver(mail.to(), body));
        })
        .register(NIGHTLY_REPORT, (ctx, none) -> buildReport());

runner.start();                                        // returns at once, the services run on their own threads

var client = runner.client();

EnqueueResult result = client.enqueue(TaskRequest.of(SEND_MAIL, new MailRequest("a@example.com", "Welcome"))
        .queue("Q:HIGH")
        .delay(Duration.ofMinutes(5))
        .uniqueKey("welcome-42")                       // a second request with this key is answered Duplicate
        .build());
if (result instanceof EnqueueResult.Duplicate d) {
    log.info("already queued as task {}", d.existingId());
}

client.putSchedule(ScheduleDefinition.of("nightly-report", NIGHTLY_REPORT, "30 2 * * *").queue("Q:LOW"));

// on shutdown
runner.stop();                                         // waits for running tasks, up to the shutdown timeout
jedis.close();
```

`render`, `deliver` and `buildReport` stand for your own code. The processor lambda receives a
`TaskContext` and the decoded payload (`Void`, so `null`, for a type without payload).

## How it works

A task is in exactly one state: QUEUED, SCHEDULED, LEASED, SUCCEEDED, DEAD, CANCELLED or EXPIRED. Every
transition between states is one atomic call on the store that checks the task's revision, so a task runs
again only when the worker holding it crashed, lost its lease or failed the attempt. Several runners on the
same store share the work without locks. The Redis store does each transition in one Lua script; the
PostgreSQL store does it in one SQL statement, with `FOR UPDATE SKIP LOCKED` for claims. The design, the
record layout, the store contract and both storage layouts are in [docs/design-3.0.md](docs/design-3.0.md).

```
enqueue --(no due)--> QUEUED --claim--> LEASED --ack--> SUCCEEDED
   |                     ^                |  |
   +--(due)--> SCHEDULED --promote--+     |  +--fail, attempts left--> SCHEDULED (retry time)
                  ^                       |  +--fail, exhausted-----> DEAD
                  +-------reschedule------+  +--lease expired-------> retried by maintenance
```

CANCELLED and EXPIRED are reserved: nothing moves a task there in this version.

A runner is made of four services: the task processor (claims in batches and runs the tasks, renews their
leases), the scheduled task service (promotes due tasks), the recurring task service (creates occurrences of
recurring definitions) and the maintenance service (retries expired leases, purges finished tasks, releases
unique keys). A monitor restarts a service whose thread died.

## Usage

### Task types and processors

`TaskType.of(name, PayloadClass.class)` encodes the payload with the runner's JSON mapper;
`TaskType.of(name, codec)` takes a `PayloadCodec` for another format; `TaskType.of(name)` is a type without
payload. `payloadVersion(n)` is stored with each task, so a processor can read old tasks differently after
the payload shape changed.

Delivery is at least once, so a processor must be safe to repeat. What a processor does decides what
happens next:

| Processor | Effect |
|---|---|
| returns normally | the task succeeded |
| throws `TaskRescheduleException(delay)` | runs again after the delay, the attempt is not counted |
| throws `TaskUnrecoverableException` | the task is dead at once |
| throws any other exception | the attempt failed; the retry policy decides the delay, or dead when attempts are used up |

A failed attempt is recorded with the task (the last 20), with the exception type, message and stack.
`TaskContext` also gives `id()`, `attempt()`, `headers()`, `task()` (a `TaskView`) and `progress(text)`.

### Steps

`ctx.step(name, ...)` runs its body once per task, not once per attempt. The result is stored before `step`
returns; on a retry the stored result is returned and the body does not run. A step is repeated only when
the worker dies between finishing the body and storing the result, so put side effects inside steps and
keep each step idempotent. Work outside a step runs on every attempt. Step results are encoded with the
runner's JSON mapper; a step name is at most 64 characters, a result at most 8192 characters and all steps of
a task at most 64 KiB.

### Enqueueing

`TaskRequest.of(type, payload)` takes these options:

| Option | Meaning |
|---|---|
| `queue(name)` | target queue; the default queue when absent |
| `delay(duration)` / `at(instant)` | run later; the last one set wins |
| `uniqueKey(key)` / `uniqueKey(key, Uniqueness.keepFor(d))` | at most one task holds the key; released when the task finishes, or `d` after |
| `maxAttempts(n)` | attempts before the task is dead; the runner default when absent |
| `account(text)`, `header(name, value)` | application labels, stored with the task (8 KiB of headers) |
| `id(id)` | an id from `client.nextId()`, see below |

`enqueue` returns `Accepted(id)` or `Duplicate(existingId)`; `enqueueAll` stores many requests in chunks.
Payloads are limited to 256 KiB. A request also accepts `startBy`, `maxRun` and `retryPolicy`; they are
stored with the task but not acted on in this version.

If the store cannot be reached, `enqueue` throws `EnqueueFailedException` and the caller does not know
whether the task was stored. Ids are allocated by the client before the write, so the retry is safe:

```java
long id = client.nextId();
var request = TaskRequest.of(SEND_MAIL, mail).id(id).build();
try {
    client.enqueue(request);
} catch (EnqueueFailedException e) {
    client.enqueue(request);        // same id: stored once, answered Accepted either way
}
```

### Recurring tasks

`client.putSchedule(ScheduleDefinition.of(id, type, payload, cron))` creates or replaces a definition and
returns the next fire time; `.queue(...)`, `.account(...)` and `.maxAttempts(...)` refine it. The cron
expression has 5 fields (Unix format) and is evaluated in UTC. Each fire time creates one task, exactly once
across all instances. Missed fire times are skipped: after downtime a definition fires once for the stored
fire time and then continues from now. Registering the same id with the same expression keeps the next
fire time; a changed expression resets it. `client.deleteSchedule(id)` and `client.schedules()` remove and
list definitions.

### Queues

A runner processes its queues round robin, one claim covering all of them; a queue listed twice gets two
turns. The default queues are `Q:DEF`, `Q:LOW`, `Q:MED` and `Q:HIGH`. Configure your own:

```java
BackgroundRunner.builder()
        .configureQueues(List.of("default", "mail", "reports"),     // queues tasks can be enqueued to
                         List.of("default", "mail"),                // queues this instance processes
                         "default")                                 // queue of requests that name none
```

### Retries

The default retry policy waits 1 s after the first failed attempt, doubles per attempt, caps at 1 hour and
varies each delay by up to 20 percent. Replace it with `retryPolicy(RetryPolicy.exponential(base, cap))`,
`RetryPolicy.fixed(delay)` or your own `RetryPolicy`, which returns `RetryAfter(delay)` or `Dead(reason)`.
The runner ends the task as dead anyway when the attempts are used up (10 by default).

### Observing a runner

A `TaskObserver` receives `TaskEvent`s (`Started`, `Succeeded`, `Retrying`, `Died`, `Rescheduled`,
`LeaseExpired`, `Deferred`, `ScheduleFired`, `Purged`). It runs on the thread that produced the event,
after the outcome was applied, and an exception in it never changes a task:

```java
BackgroundRunner.builder()
        .observer(event -> {
            switch (event) {
                case TaskEvent.Died d -> alerts.send(d.task() + ": " + d.reason());
                case TaskEvent.Retrying r -> log.warn("{} retries at {}", r.task(), r.retryAt());
                default -> { }
            }
        })
```

### Inspection

```java
client.task(id);                                  // Optional<TaskView>, any state
client.counts();                                  // per queue name: counts per state
client.tasks(TaskState.DEAD, "mail", 0, 100);     // oldest state change first; afterId for paging
```

A `TaskView` carries state, attempts, due time, the last failures, completed steps and progress.

### Retention

Finished tasks are kept for inspection, then purged by the maintenance service: succeeded tasks 48 hours
(their payload is dropped unless `keepSucceededPayload(true)`), dead tasks 90 days. A retention of
`Duration.ZERO` removes the task at the transition instead. A unique key with `keepFor` keeps its task
until the window ends.

### Concurrency

By default a runner runs twice as many tasks at once as there are processors, each on a virtual thread.
`concurrency(Concurrency.fixed(n))` sets another limit; `concurrency(Concurrency.adaptive(min, max))` lets
the limit move with CPU load and throughput.

## Configuration

All settings are on `BackgroundRunner.builder()`.

| Setting | Default | Meaning |
|---|---|---|
| `store` | required | `new RedisTaskStore(jedis, "namespace")`, `new PostgresTaskStore(dataSource, "prefix")` or `new InMemoryStore()` |
| `registry` | new registry | the processor registry; by default created by the runner and reached with `runner.registry()` |
| `jsonMapper` | none | payloads and step results as JSON; needed unless every task type has its own codec |
| `clock` | `Clock.systemUTC()` | clock for client-side instants (`created`, delays, retry times) |
| `concurrency` | `Concurrency.fixed(max(2, 2 x processors))` | how many tasks run at once; `fixed(n)` or `adaptive(min, max)` |
| `taskExecutor` | built from `concurrency` | a custom `BackgroundExecutor`; the runner shuts it down |
| `retryPolicy` | exponential, 1 s doubling, capped at 1 h, 20 % jitter | delay after a failed attempt |
| `observer` | none | receives every `TaskEvent` |
| `workerLabel` | JVM name (`pid@host`) | identifies this process in the worker registry; 1 to 128 characters |
| `threadGroupName` | `bgr` | name of the runner's thread group |
| `succeededRetention` | 48 hours | how long succeeded tasks are kept; zero removes them at once |
| `deadRetention` | 90 days | how long dead tasks are kept; zero removes them at once |
| `cancelledRetention` | 48 hours | how long cancelled and expired tasks are kept |
| `keepSucceededPayload` | false | keep the payload of succeeded tasks (dropped on success by default) |
| `purgeInterval` | 1 minute | time between purge runs |
| `purgeBatchSize` | 500 | tasks removed per purge call; the purge loops until a call removes fewer |
| `leaseDuration` | 60 s (at least 1 s) | how long a claimed task stays leased without renewal; renewed every third while running |
| `idlePollInterval` | 500 ms (at least 10 ms) | sleep after a claim that found nothing, and the wait for a free executor slot |
| `claimBatchSize` | 32 | most tasks claimed in one call, bounded by free executor slots |
| `shutdownTimeout` | 30 s | how long `stop()` waits for services and running tasks |
| `serviceCheckInterval` | 10 s | how often a dead service thread is restarted |
| `maintenanceCheckInterval` | 5 s (at least 1 s) | how often expired leases are looked for |
| `scheduledTasksCheckInterval` | 1 s (at least 10 ms) | upper bound of the wait between promotions of due scheduled tasks; it wakes earlier for the next due task |
| `recurringTasksCheckInterval` | 10 s | how often recurring definitions are checked |
| `idBlockSize` | 64 (1 to 100000) | task ids the client reserves per store call |
| `defaultMaxAttempts` | 10 (1 to 32767) | attempts for requests that do not set `maxAttempts` |
| `configureQueues` | `Q:DEF`, `Q:LOW`, `Q:MED`, `Q:HIGH`; default queue `Q:DEF` | available queues, queues this instance processes (round robin), default queue |

`start()` fails with `TaskConfigurationError` when no processor is registered. `quiet()` stops claiming while
running tasks finish and `resume()` claims again. `stop()` stops the services, waits up to `shutdownTimeout`
and keeps renewing leases of running tasks meanwhile; tasks still running afterwards are retried when their
lease expires. A process that only produces tasks can use `TaskClient.builder().store(store).jsonMapper(mapper).build()`.

## Stores

### Redis

```java
var store = new RedisTaskStore(jedis, "my-app");   // UnifiedJedis (for example a RedisClient) and a namespace
```

The namespace prefixes every key and carries the Redis Cluster hash tag, so all keys of a store share one
slot. The store does not close the Jedis client. Redis 7.0 or newer is required.

### PostgreSQL

```java
var store = new PostgresTaskStore(dataSource, "bth");   // tables bth_task, bth_queue, bth_event, bth_worker, bth_schedule, bth_meta
```

The tables are created when the runner or client is built, or run `store.schemaStatements()` from your
migration tool. The prefix is lower case letters, digits and underscores, at most 41 characters; runners with
the same prefix on the same database share the work. There are no foreign keys. PostgreSQL 9.5 or newer is
required (`SKIP LOCKED`, `ON CONFLICT`); the tests run on 17.

### Other stores

The store interface is `today.bonfire.oss.bth4j.spi.FullStore`. A store on another engine implements it and
subclasses `StoreContract` from `bth4j-store-tests`; see "The store SPI and how to implement a store" in
[docs/design-3.0.md](docs/design-3.0.md).

## Testing

`bth4j-testing` provides `InMemoryStore`, a store that keeps everything in memory (one lock, no server), and
`MutableClock`, a clock a test moves by hand. Both the store and the runner take the clock, so due times,
leases and retention follow it:

```java
var clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
var store = new InMemoryStore(clock);

var runner = BackgroundRunner.builder()
        .store(store)
        .clock(clock)
        .jsonMapper(new JacksonMapper())
        .idlePollInterval(Duration.ofMillis(10))
        .build();
runner.registry().register(SEND_MAIL, (ctx, mail) -> { /* ... */ });
runner.start();

runner.client().enqueue(TaskRequest.of(SEND_MAIL, new MailRequest("a@example.com", "Hi"))
        .delay(Duration.ofHours(1))
        .build());
clock.advance(Duration.ofHours(1));                 // the task is promoted and runs
```

`new InMemoryStore()` uses the system clock. The in-memory store passes the same `StoreContract` as the
Redis and PostgreSQL stores, so behaviour in tests matches production. The services still wait in real
time between their checks, so a test that moves the clock should poll for the result (for example with
Awaitility) instead of asserting at once.

## Upgrading from 2.x

3.0 cannot read 2.x data. Let the 2.x runners drain their queues, then start 3.0, or use a new Redis
namespace. The API was redesigned: `Event` and `Task` are replaced by `TaskType` and `TaskRequest`, `TaskOps`
by `TaskClient`, and the runner takes a store instead of a Jedis client and a namespace prefix. The list of
changes is in [CHANGELOG.md](CHANGELOG.md).

## Limitations

- Cron expressions use the 5 field Unix format and are evaluated in UTC.
- Cancelling a task and expiring it after a start-by deadline are not available yet; `startBy`, `maxRun`
  and `TaskRequest.retryPolicy` are stored but not acted on.
- There is no admin API or dashboard; use `TaskClient` for inspection.
- The Redis store needs Redis 7.0 or newer, the PostgreSQL store PostgreSQL 9.5 or newer, and Java 21 or
  later is required.

## Contributing

- Build and test with the Maven Wrapper: `./mvnw verify`. Java 21 or later is required.
- The build has the modules `bth4j-core`, `bth4j-redis`, `bth4j-postgres` and `bth4j-testing`, released together under one version, plus `bth4j-store-tests` (the contract tests, not published). Unit tests run in the `test` phase. The integration tests (`*IT` in the store modules) start a Redis or PostgreSQL container with Testcontainers in the `integration-test` phase and need Docker.
- Versioning: the first change after a release bumps the version in `pom.xml` to the next `-SNAPSHOT`. A release is a pull request titled `Release X.Y.Z` that sets the final version and adds a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md`; merging it publishes the release.
