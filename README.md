# Background Task Handler (BTH4J)

A Java library for background tasks with at-least-once delivery. Tasks live in a store (Redis or
PostgreSQL), several instances share the work through that store, and every state change is one atomic
transition guarded by a revision, so a worker that lost its task cannot change it.

## Overview

BTH4J handles three kinds of work:

* **Immediate tasks:** enqueued and run as soon as a worker is free.
* **Scheduled tasks:** enqueued with a delay or a run time.
* **Recurring tasks:** a cron or fixed-delay definition that creates one task per fire time.

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

The metrics and the admin HTTP server are separate modules; add them when you use them:

```xml
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-micrometer</artifactId>
    <version>3.0.0</version>
</dependency>
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-admin-http</artifactId>
    <version>3.0.0</version>
</dependency>
```

| Module | Content |
|---|---|
| `bth4j-core` | task model, runner, services, the store interface (`today.bonfire.oss.bth4j.spi`) |
| `bth4j-redis` | `RedisTaskStore`: Redis 7.0 or newer or Valkey, also on Redis Cluster; uses Jedis. Tested on Redis 8.8 and Valkey 9.1.3 |
| `bth4j-postgres` | `PostgresTaskStore`: PostgreSQL 9.5 or newer, tested on 18; uses only `java.sql`, the application supplies the JDBC driver and a pooled `DataSource` |
| `bth4j-testing` | `InMemoryStore` and `MutableClock` for tests of applications |
| `bth4j-micrometer` | `MicrometerTaskObserver` and `MicrometerRunnerMetrics`; `micrometer-core` is a provided dependency, the application supplies it |
| `bth4j-admin-http` | `AdminHttpServer`: a JSON admin API over `AdminOps` on the JDK HTTP server, with users, roles and sessions; no dependencies besides `bth4j-core` |
| `bth4j-store-tests` | `StoreContract` and `RunnerContract`, the tests every store must pass (not published) |

The `dashboard/` directory is not a Maven module: it holds the operator dashboard, a static Vue app that the
admin HTTP server can serve (see "Admin HTTP server and dashboard").

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

A task also ends CANCELLED (an operator cancelled it, its processor threw `TaskCancelledException` or it ran
past its `maxRun`) or EXPIRED (its `startBy` passed before it could start). From DEAD, CANCELLED or EXPIRED
an operator can send it back to QUEUED with `AdminOps.retry`.

A runner is made of four services: the task processor (claims in batches and runs the tasks, renews their
leases), the scheduled task service (promotes due tasks), the recurring task service (creates occurrences of
recurring definitions) and the maintenance service (retries expired leases, expires tasks past their
`startBy`, writes the worker heartbeat, purges finished tasks, releases unique keys, removes stale workers). A monitor restarts a service whose thread died.

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
| `startBy(instant)` | the task expires if it has not started by then, see "Cancelling tasks"; must not be before the run time |
| `maxRun(duration)` | an attempt running longer is asked to stop, see "Cancelling tasks" |
| `retryPolicy(name)` | a retry policy registered on the runner with that name, see "Retries" |
| `account(text)`, `header(name, value)` | application labels, stored with the task (8 KiB of headers) |
| `id(id)` | an id from `client.nextId()`, see below |

`enqueue` returns `Accepted(id)` or `Duplicate(existingId)`; `enqueueAll` stores many requests in chunks.
Payloads are limited to 256 KiB.

`client.prepare(request)` validates and encodes a request into a `TaskRecord` with its id allocated, without
writing it. With PostgreSQL, `PostgresTaskStore.enqueue(Connection, TaskRecord)` stores that record on your
own connection and does not commit, so the task exists only if your transaction commits:

```java
TaskRecord record = client.prepare(TaskRequest.of(SEND_MAIL, mail).build());
try (Connection c = dataSource.getConnection()) {
    c.setAutoCommit(false);
    // ... your own writes on c ...
    postgresStore.enqueue(c, record);
    c.commit();
}
```

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

### Cancelling tasks

An operator cancels a task through `AdminOps` (see "Admin API"), with the revision it saw:

```java
AdminOps admin = runner.admin();
TaskView task = admin.task(id).orElseThrow();
CancelOutcome outcome = admin.cancel(task.id(), task.revision(), "customer withdrew the order");
```

A QUEUED or SCHEDULED task becomes CANCELLED at once (`CANCELLED`). A LEASED task only gets the request
(`REQUESTED`): the running runner sees it on the next lease renewal, within a third of `leaseDuration`, and
the task ends CANCELLED when the processor stops. `STALE` means the revision or state had changed and nothing
was done. The reason (default "cancelled by operator", at most 512 characters) stays on the task.

The processor decides how to stop. `ctx.cancelRequested()` shows the request and `ctx.checkCancelled()`
throws `TaskCancelledException`, which ends the task CANCELLED. A task type declared with `interruptOnCancel()`
also has its thread interrupted, so sleeps and blocking calls wake up; the exception that follows is treated
as the cancellation:

```java
static final TaskType<Export> EXPORT = TaskType.of("export", Export.class).interruptOnCancel();

runner.registry().register(EXPORT, (ctx, export) -> {
    for (var part : export.parts()) {
        ctx.checkCancelled();                     // ends the task as cancelled when an operator asked
        write(part);
    }
});
```

Starting a new `ctx.step` also checks. A processor that never checks and is not interruptible runs to its end
and the task ends SUCCEEDED; the request is not applied after a normal return. If the worker dies while a cancel is pending, maintenance
ends the task CANCELLED when its lease expires instead of retrying it.

`maxRun(duration)` uses the same path: when an attempt has run longer, the lease renewal asks it to stop with
the reason "max run time ... exceeded". `startBy(instant)` expires a task that did not start in time:
claims skip it, maintenance moves it to EXPIRED and the observer receives `TaskEvent.Expired`.

```java
client.enqueue(TaskRequest.of(EXPORT, export)
        .startBy(Instant.now().plus(Duration.ofMinutes(10)))   // expired, not run, if no worker was free by then
        .maxRun(Duration.ofMinutes(30))                        // asked to stop after 30 minutes
        .build());
```

### Recurring tasks

`client.putSchedule(ScheduleDefinition.of(id, type, payload, cron))` creates or replaces a definition and
returns the next fire time; `.queue(...)`, `.account(...)` and `.maxAttempts(...)` refine it. The cron
expression has 5 fields (Unix format). Each fire time creates one task, exactly once across all instances.
Registering the same id with the same timing (expression, zone and delay) keeps the next fire time; changed
timing resets it. `client.deleteSchedule(id)` and `client.schedules()` remove and list definitions.

More options on `ScheduleDefinition`:

```java
// cron in a time zone (UTC by default); a fire time that does not exist in a DST gap moves to the next valid
// instant, a repeated hour fires once
client.putSchedule(ScheduleDefinition.of("daily-digest", DIGEST, "0 8 * * *")
        .zone(ZoneId.of("Europe/Berlin")));

// misfire: what happens when a fire time was missed by more than the threshold (below)
client.putSchedule(ScheduleDefinition.of("hourly-sync", SYNC, "0 * * * *")
        .misfire(Misfire.catchUp(3)));          // Misfire.SKIP, Misfire.FIRE_ONCE_NOW (default), Misfire.catchUp(max)

// overlap: no new occurrence while the previous one has not finished
client.putSchedule(ScheduleDefinition.of("nightly-report", NIGHTLY_REPORT, "30 2 * * *")
        .overlap(Overlap.SKIP));

// fixed delay: the next occurrence is due one delay after the previous one succeeded
client.putSchedule(ScheduleDefinition.every("poll-feed", POLL_FEED, Duration.ofMinutes(5)));
```

The misfire threshold is twice `recurringTasksCheckInterval`, at least one minute. `SKIP` creates nothing and
continues from now, `FIRE_ONCE_NOW` creates one task now, `CATCH_UP` creates one task per missed fire time up
to `max` (the most recent ones when more were missed) and continues from now. Every misfire is recorded as an
issue. With `Overlap.SKIP`, an occurrence is skipped (and an issue `schedule-overlap` recorded) while the task
of the previous one is not finished. A fixed-delay definition (at least 1 second) first fires one delay after
registration; if an occurrence ends DEAD or CANCELLED (in its processor, through an expired lease or by an
operator cancel) the definition is paused and an issue `schedule-paused` is recorded, so a failing job does
not run again until an operator resumes it. Pausing,
resuming and triggering a definition are `AdminOps` operations; a paused cron definition keeps its stored next
fire time.

### Queues

A runner processes its queues round robin, one claim covering all of them; a queue listed twice gets two
turns. The default queues are `Q:DEF`, `Q:LOW`, `Q:MED` and `Q:HIGH`. Configure your own:

```java
BackgroundRunner.builder()
        .configureQueues(List.of("default", "mail", "reports"),     // queues tasks can be enqueued to
                         List.of("default", "mail"),                // queues this instance processes
                         "default")                                 // queue of requests that name none
```

### Queue policies and pausing

A `QueuePolicy(weight, maxConcurrent)` shares a runner's free executor slots between its queues. Free slots
are divided in proportion to the weights (default 1), each queue is capped at `maxConcurrent` tasks of that
queue running on this runner at once (0 means no limit), and a queue that cannot fill its share lets the others
use it. A runner with any policy claims once per queue instead of once per cycle; queues without a policy have
weight 1 and no limit. Limits are per runner, not per fleet.

```java
BackgroundRunner.builder()
        .configureQueues(List.of("default", "mail", "reports"), List.of("default", "mail", "reports"), "default")
        .queue("mail", QueuePolicy.weight(3))                   // three times the share of the others
        .queue("reports", QueuePolicy.maxConcurrent(2))         // never more than two reports at once here
        .queue("default", new QueuePolicy(2, 0))
```

`admin.pauseQueue("mail", true)` makes every runner stop claiming from that queue, and
`admin.pauseQueue("mail", false)` resumes it. Enqueueing to a paused queue still works. A runner notices the
change on its next claim.

### Retries

The default retry policy waits 1 s after the first failed attempt, doubles per attempt, caps at 1 hour and
varies each delay by up to 20 percent. Replace it with `retryPolicy(RetryPolicy.exponential(base, cap))`,
`RetryPolicy.fixed(delay)` or your own `RetryPolicy`, which returns `RetryAfter(delay)` or `Dead(reason)`.
The runner ends the task as dead anyway when the attempts are used up (10 by default).

Register policies under a name and let a request choose one:

```java
BackgroundRunner.builder()
        .retryPolicy("slow-api", RetryPolicy.exponential(Duration.ofMinutes(1), Duration.ofHours(6)))
        ...

client.enqueue(TaskRequest.of(SEND_MAIL, mail).retryPolicy("slow-api").build());
```

A task that names a policy the runner does not have uses the default policy, and the runner records a
`retry-policy` issue.

### Observing a runner

A `TaskObserver` receives `TaskEvent`s (`Started`, `Succeeded`, `Retrying`, `Died`, `Rescheduled`,
`Cancelled`, `Expired`, `LeaseExpired`, `Deferred`, `ScheduleFired`, `Purged`). It runs on the thread that produced the event,
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

`new LoggingTaskObserver()` is a ready observer that writes one SLF4J line per event (logger `bth4j.tasks`) and
puts `bth4j.task`, `bth4j.event` and `bth4j.queue` in the MDC; pass your own `Logger` to use another name.

### Inspection

```java
client.task(id);                                  // Optional<TaskView>, any state
client.counts();                                  // per queue name: counts per state
client.tasks(TaskState.DEAD, "mail", 0, 100);     // oldest state change first; afterId for paging
```

A `TaskView` carries state, attempts, due time, the last failures, completed steps and progress.

### Health and issues

```java
RunnerHealth health = runner.health();     // status NEW, RUNNING, QUIET or STOPPED; workerId, running tasks,
                                           // lastClaimAt, lastMaintenanceAt, lastStoreErrorAt, lastStoreError, issueCount
List<Issue> issues = runner.issues();      // the last 100 things an operator should look at, oldest first
boolean drained = runner.drain(Duration.ofSeconds(30));   // quiet, then wait for the running tasks
```

An `Issue` has a time, a kind, a detail and the task id when there is one. The kinds are `unknown-event`,
`store-error`, `lease-lost`, `retry-policy`, `executor-rejected`, `schedule-misfire`, `schedule-overlap` and
`schedule-paused`. Issues live in the memory of the runner that saw them. `drain` leaves the runner quiet
(call `resume()` to claim again, or `stop()`), and returns whether every running task finished in time; use it
before a rolling restart.

Each runner also writes a heartbeat (last seen time and running tasks) to the worker registry every
`maintenanceCheckInterval`, and maintenance deletes workers that were silent for `workerStaleAfter` (10
minutes by default). `admin.workers()` lists them.

### Admin API

`AdminOps` is the operator's view and set of operations over one store. `runner.admin()` also reports that
runner's health and issues; `client.admin()` works in a process without a runner, where `health()` is empty and
`issues()` is an empty list.

```java
AdminOps admin = runner.admin();

admin.counts();                                    // Map<queue name, StateCounts>
admin.queues();                                    // id, name, paused
admin.tasks(TaskState.DEAD, "mail", 0, 100);       // oldest state change first, afterId for paging; queue null for all
admin.task(id);                                    // Optional<TaskView>
admin.retry(id, revision);                         // DEAD, CANCELLED or EXPIRED back to QUEUED, attempts reset
admin.cancel(id, revision, "reason");              // CANCELLED, REQUESTED or STALE
admin.delete(id, revision);                        // any state but LEASED
admin.pauseQueue("mail", true);
admin.schedules();                                 // ScheduleView: cron, zone, misfire, fixed delay, paused, next fire
admin.pauseSchedule("poll-feed", false);           // resume; a fixed-delay definition is re-armed one delay from now
admin.triggerSchedule("nightly-report");           // fires at the next recurring check
admin.deleteSchedule("nightly-report");
admin.workers();
admin.purgeStaleWorkers(Duration.ofMinutes(10));
```

Every change to a task takes the revision the operator saw. If the task changed meanwhile, nothing is done and
the call returns `false` or `STALE`. A retried task keeps its failures and steps, but loses its unique key (the
key was released when the task ended and may belong to another task now) and its `startBy`, so a retried
expired task does not expire again.

### Metrics

`bth4j-micrometer` needs `micrometer-core` on the classpath. Register the observer for events and bind the
gauges for state:

```java
MeterRegistry registry = ...;                      // your Micrometer registry

var runner = BackgroundRunner.builder()
        .store(store)
        .observer(new MicrometerTaskObserver(registry, Tags.of("app", "billing")))
        .build();
...
runner.start();
MicrometerRunnerMetrics metrics = MicrometerRunnerMetrics.bind(runner, registry, Duration.ofSeconds(15));
// on shutdown: metrics.close();
```

| Meter | Type | Tags |
|---|---|---|
| `bth4j.tasks` | counter | `event`, `queue`, `result` (`succeeded`, `retried`, `died`, `rescheduled`, `cancelled`, `expired`, `lease_lost`), `outcome` (`applied`, `lost`) |
| `bth4j.task.duration` | timer | `event`, `queue` |
| `bth4j.deferred` | counter | `event` |
| `bth4j.schedule.fired` | counter | none |
| `bth4j.purged` | counter | `state` |
| `bth4j.runner.running` | gauge | none |
| `bth4j.runner.up` | gauge | none; 1 while the runner is RUNNING |
| `bth4j.tasks.state` | gauge | `state`, `queue` |

The observer's tags are only names and results, never ids or messages, so the number of meters stays bounded.
`expired` and `lease_lost` are counted with `event` and `queue` set to `unknown`. The gauges are refreshed by
one daemon thread every `refresh`, so a scrape never reads the store.

### Admin HTTP server and dashboard

`bth4j-admin-http` serves `AdminOps` as a JSON API under `/api/v1` on the JDK HTTP server:

```java
var server = AdminHttpServer.builder()
        .admin(runner.admin())
        .bind("127.0.0.1", 8787)                          // the default
        .user("ops", System.getenv("BTH4J_OPS_PASSWORD"), Role.ADMIN)
        .user("support", System.getenv("BTH4J_SUPPORT_PASSWORD"), Role.VIEWER)
        .staticDir(Path.of("dashboard"))                  // serves the dashboard at http://127.0.0.1:8787/
        .build();
server.start();
// on shutdown: server.stop();
```

At least one user is required. `Role.ADMIN` may read and change; `Role.VIEWER` may read everything, including
task payloads, and every mutation answers 403. A client signs in with `POST /api/v1/session` (`{"user", "password"}`),
receives the `bth4j_session` cookie (`HttpOnly`, `SameSite=Strict`) and a CSRF token in the reply, and sends the token
as `X-CSRF-Token` on every POST and DELETE. Sessions are kept in memory, last 12 hours after the last request
(`sessionTtl`) and end when the server stops. Five failed logins in a row block that remote address for 10
seconds (HTTP 429). A stale revision is answered 409, an unknown id 404; task ids and revisions are JSON strings.
The endpoints are `me`, `session`, `health`, `overview`, `queues` (with `pause` and `resume`), `tasks` (list
with `state`, `queue`, `after` and `limit` up to 200, one task, its `payload`, `retry`, `cancel`, delete),
`schedules` (with `pause`, `resume`, `trigger`, delete), `workers` and `issues`.

The server speaks plain HTTP unless `https(SSLContext)` is set, which also marks the cookie `Secure`; keep it
on the loopback interface or behind TLS. `corsOrigin("https://dashboard.example.com")` lets a browser app on
another origin call the API with credentials.

The dashboard is the static Vue 3 app in `dashboard/` (no build step; it loads Vue from jsDelivr at a pinned
version, or from a local copy for air-gapped hosts). It shows overview, queues, tasks with filters and detail
(timeline, steps, failures, payload on request), dead tasks, schedules, workers and issues, and offers retry,
cancel (with a reason), delete, pause and trigger to admins. The files come from the repository checkout:
point `staticDir` at the `dashboard` directory, or copy it to your own static host and set
`window.BTH4J_API` in `index.html` (the dashboard and API hosts must then share a registrable domain because of
the `SameSite=Strict` cookie). Details are in [dashboard/README.md](dashboard/README.md). `dashboard/dev/fake-api.py`
serves sample data without Java.

### Retention

Finished tasks are kept for inspection, then purged by the maintenance service: succeeded tasks 10 minutes
(their payload is dropped unless `keepSucceededPayload(true)`), dead tasks 90 days, cancelled and expired
tasks 48 hours. A retention of
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
| `retryPolicy(name, policy)` | none | adds a named policy that requests choose with `TaskRequest.retryPolicy(name)`; name 1 to 64 characters; may be called several times |
| `queue(name, QueuePolicy)` | `QueuePolicy.DEFAULT` (weight 1, no limit) | share of free slots and concurrency limit of a queue on this runner |
| `observer` | none | receives every `TaskEvent` |
| `workerLabel` | JVM name (`pid@host`) | identifies this process in the worker registry; 1 to 128 characters |
| `threadGroupName` | `bgr` | name of the runner's thread group |
| `succeededRetention` | 10 minutes | how long succeeded tasks are kept; zero removes them at once |
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
| `maintenanceCheckInterval` | 5 s (at least 1 s) | how often expired leases are looked for, tasks past `startBy` are expired and the worker heartbeat is written |
| `workerStaleAfter` | 10 minutes | worker registry rows without a heartbeat for this long are deleted by the purge |
| `scheduledTasksCheckInterval` | 1 s (at least 10 ms) | upper bound of the wait between promotions of due scheduled tasks; it wakes earlier for the next due task |
| `recurringTasksCheckInterval` | 10 s | how often recurring definitions are checked; twice this value (at least 1 minute) is the misfire threshold |
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
subclasses `StoreContract` and `RunnerContract` from `bth4j-store-tests`; see "The store SPI and how to implement a store" in
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

The integration tests of the store modules start containers with Testcontainers: `redis:8.8-alpine` and
`postgres:18-alpine` by default. `./mvnw -pl bth4j-redis -am verify -Pvalkey` runs the Redis suite against
`valkey/valkey:9.1.3-alpine`, and `-Dbth4j.redis.image=...` or `-Dbth4j.postgres.image=...` selects any
other image.

## Upgrading from 2.x

3.0 cannot read 2.x data. Let the 2.x runners drain their queues, then start 3.0, or use a new Redis
namespace. The API was redesigned: `Event` and `Task` are replaced by `TaskType` and `TaskRequest`, `TaskOps`
by `TaskClient`, and the runner takes a store instead of a Jedis client and a namespace prefix. The list of
changes is in [CHANGELOG.md](CHANGELOG.md).

## Limitations

- Cron expressions use the 5 field Unix format.
- Cancelling a running task is cooperative: the processor has to call `checkCancelled()` or be declared with
  `interruptOnCancel()`, and it is noticed on the next lease renewal.
- `QueuePolicy` limits apply to one runner, not to the fleet; there are no fleet-wide concurrency limits, rate
  limits or priority bands.
- Issues are kept in the memory of the runner that recorded them (the last 100) and are lost on restart.
- Admin sessions of `AdminHttpServer` are in memory and the server has no user store beyond the users given to
  the builder; put TLS (`https`) or a TLS-terminating proxy in front of it when it is not on the loopback interface.
- The Redis store needs Redis 7.0 or newer, the PostgreSQL store PostgreSQL 9.5 or newer, and Java 21 or
  later is required.

## Contributing

- Build and test with the Maven Wrapper: `./mvnw verify`. Java 21 or later is required.
- The build has the modules `bth4j-core`, `bth4j-redis`, `bth4j-postgres`, `bth4j-micrometer`, `bth4j-admin-http` and `bth4j-testing`, released together under one version, plus `bth4j-store-tests` (the contract tests, not published). Unit tests run in the `test` phase. The integration tests (`*IT` in the store modules) start a Redis or PostgreSQL container with Testcontainers in the `integration-test` phase and need Docker.
- Versioning: the first change after a release bumps the version in `pom.xml` to the next `-SNAPSHOT`. A release is a pull request titled `Release X.Y.Z` that sets the final version and adds a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md`; merging it publishes the release.
