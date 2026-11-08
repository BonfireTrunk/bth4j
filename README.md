# Background Task Handler (BTH4J)

A Java library for background tasks with at-least-once delivery. Tasks live in a store (Redis, PostgreSQL or
SQLite), several instances share the work through that store, and every state change is one atomic
transition guarded by a revision, so a worker that lost its task cannot change it.

## Overview

BTH4J handles three kinds of work:

* **Immediate tasks:** enqueued and run as soon as a worker is free.
* **Scheduled tasks:** enqueued with a delay or a run time.
* **Recurring tasks:** a cron or fixed-delay definition that creates one task per fire time.

A task has a type (`TaskType`) with a payload, a queue, an optional unique key, a retry budget and, while
running, a lease that the runner renews. A processor can split its work into durable steps so that a retry
resumes where the failed attempt stopped. The store is an interface with a Redis, a PostgreSQL, a SQLite and an
in-memory implementation.

## Installation

Add `bth4j-core` and one store module. Add `bth4j-testing` for tests.

```xml
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-core</artifactId>
    <version>4.1.0</version>
</dependency>
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-redis</artifactId>       <!-- or bth4j-postgres or bth4j-sqlite -->
    <version>4.1.0</version>
</dependency>
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-testing</artifactId>
    <version>4.1.0</version>
    <scope>test</scope>
</dependency>
```

The metrics and the admin HTTP server are separate modules; add them when you use them:

```xml
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-micrometer</artifactId>
    <version>4.1.0</version>
</dependency>
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j-admin-http</artifactId>
    <version>4.1.0</version>
</dependency>
```

| Module | Content |
|---|---|
| `bth4j-core` | task model, runner, services, the store interface (`today.bonfire.oss.bth4j.spi`) |
| `bth4j-redis` | `RedisTaskStore`: Redis 7.4 or newer or Valkey 9 or newer; uses Jedis. Tested on standalone Redis 8.8 and Valkey 9.1.3; Redis Cluster is not tested (see "Redis") |
| `bth4j-postgres` | `PostgresTaskStore`: PostgreSQL 16 or newer, 17 or newer recommended, tested on 18; uses only `java.sql`, the application supplies the JDBC driver and a pooled `DataSource` |
| `bth4j-sqlite` | `SqliteTaskStore`: SQLite 3.45 or newer in WAL mode, latest recommended, tested on 3.54; uses only `java.sql`, the application supplies the JDBC driver and a pooled `DataSource`; for runners on one machine |
| `bth4j-testing` | `InMemoryStore` and `MutableClock` for tests of applications |
| `bth4j-micrometer` | `MicrometerTaskObserver` and `MicrometerRunnerMetrics`; `micrometer-core` is a provided dependency, the application supplies it |
| `bth4j-admin-http` | `AdminHttpServer`: a JSON admin API over `AdminOps` on the JDK HTTP server, with users, roles and sessions; no dependencies besides `bth4j-core` |
| `bth4j-store-tests` | `StoreContract` and `RunnerContract`, the tests every store must pass (not published) |

The `dashboard/` directory is not a Maven module: it holds the operator dashboard, a static Vue app that the
admin HTTP server can serve (see "Admin HTTP server and dashboard").

Requires Java 21.

## Quick start

A payload is converted to text by a `JsonMapper`, a two-method interface that wraps the JSON library you
already use. With Jackson (2.12 or newer for records):

```java
public class JacksonMapper implements JsonMapper {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override public <T> T fromJson(String json, Class<T> type) throws Exception { return mapper.readValue(json, type); }
    @Override public String toJson(Object o) throws Exception { return mapper.writeValueAsString(o); }
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
past its `maxRunTime`) or EXPIRED (its `startBy` passed before it could start). From DEAD, CANCELLED or EXPIRED
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
| throws any other exception or `Error` | the attempt failed; the retry policy decides the delay, or dead when attempts are used up |

A failed attempt is recorded with the task (the last 20), with the exception type, message and stack.
An `Error` (`AssertionError`, `StackOverflowError`, `NoClassDefFoundError`, ...) is handled like an exception. After it has been recorded, a `VirtualMachineError` other than `StackOverflowError` (`OutOfMemoryError`, `InternalError`, `UnknownError`) is rethrown after it has been recorded, so the executor thread's uncaught exception handler still sees it. The same holds for such an error thrown by a retry policy, the store or an observer while a task runs: the task is recorded first, then the error is rethrown. This applies to task execution only; elsewhere (maintenance, the scheduler) an error from an observer or a retry policy is logged and swallowed.
`TaskContext` also gives `id()`, `attempt()`, `headers()`, `task()` (a `TaskView`) and `progress(text)`.

### Steps

`ctx.step(name, ...)` runs its body once per task, not once per attempt. The result is stored before `step`
returns; on a retry the stored result is returned and the body does not run. A step is repeated only when
the worker dies between finishing the body and storing the result, so put side effects inside steps and
keep each step idempotent. Work outside a step runs on every attempt. Step results are encoded with the
runner's JSON mapper; a step name must be nonblank and at most 64 UTF-16 code units, an encoded result at
most 8192, and all step names and encoded results of a task together at most 65536. These limits use Java
`String.length()`, not encoded bytes.

### Enqueueing

`TaskRequest.of(type, payload)` takes these options:

| Option | Meaning |
|---|---|
| `queue(name)` | target queue; the default queue when absent |
| `delay(duration)` / `at(instant)` | run later; the last one set wins |
| `uniqueKey(key)` / `uniqueKey(key, Uniqueness.keepFor(d))` | at most one task holds the key; released when the task finishes, or `d` (at most 100 years) after, also when the task itself was removed sooner (see "Retention"); an operator retry within `d` keeps it |
| `maxAttempts(n)` | attempts before the task is dead; the runner default when absent |
| `startBy(instant)` | the task expires if it has not started by then, see "Cancelling tasks"; must not be before the run time |
| `maxRunTime(duration)` | an attempt running longer is asked to stop, see "Cancelling tasks" |
| `retryPolicy(name)` | a retry policy registered on the runner with that name, see "Retries" |
| `account(text)`, `header(name, value)` | application labels, stored with the task (8 KiB of headers) |
| `id(id)` | an id the client allocated (`client.nextId()` or a retry prepared with `EnqueueFailedException.retryRequests(...)`), see below |

`enqueue` returns `Accepted(id)` or `Duplicate(existingId)`; `enqueueAll` stores many requests in chunks.
`client.taskByKey(key)` (and `admin.taskByKey(key)`) reads the task that holds a unique key now, open or finished
inside its `keepFor` window, without writing; it is empty when no task holds it, and also when the holder was
removed by its retention and only its key stays taken. `admin.cancelByKey(key, reason)` cancels that task,
and cancelling a waiting fixed-delay occurrence (key `sched:<schedule id>`) pauses its schedule, as cancelling it by id does.
A key follows the rule of `uniqueKey`: 1 to 256 characters, not blank, without U+0000.
Payloads are limited to 256 KiB.
No text the library stores may contain U+0000, which PostgreSQL cannot keep: the client refuses it in a request,
a schedule, a name or a cancel reason on every store, and records text from processor code (failure message and
stack, progress, step results) with U+FFFD in its place.

A later enqueue with the same key and a shorter `keepFor` does not shorten the holder's window.
`AdminOps.cancel` on a DEAD task answers `STALE` and leaves its key held for the window; a successful
`AdminOps.delete` frees the key at once and removes the task and its failure history.

For a repair sweep that re-enqueues work after a crash, the default until-done key is free as soon as the
task finishes, so a late enqueue can run the job again. Use `keepFor` longer than the window in which a retry
or sweep can still happen to prevent another enqueue from starting a second task after SUCCEEDED, DEAD or
CANCELLED. A DEAD holder inside that window still answers `Duplicate` with its id; inspect it with
`taskByKey` while its retention keeps it.

A running task holds its key until it ends: a successor enqueued with that key from inside its processor
answers `Duplicate`. Enqueue the successor after the task ends (and any keep window passes), or give it a
different key. A processor uses its own `TaskClient` to enqueue; `TaskContext` has no enqueue method.

A unique key protects task records, not running bodies. After lease loss, the old body can overlap a
replacement until its next check or, for types using `interruptOnCancel()`, an interruptible blocking call
after the runner detects the loss. Interrupts are cooperative too. Fence side effects with the task revision
or an idempotent target; use steps to checkpoint completed work, but keep each step safe to repeat.

`client.prepare(request)` validates and encodes a request into a `TaskRecord` with its id allocated, without
writing it. With PostgreSQL, `PostgresTaskStore.enqueue(Connection, TaskRecord)` stores that record on your
own connection and does not commit, so the task exists only if your transaction commits. This path skips
`TaskClient.enqueue`/`enqueueAll` checks of caller-id allocation and stored content; handle the store's raw
`EnqueueOutcome`, including `Exists`, yourself:

```java
TaskRecord record = client.prepare(TaskRequest.of(SEND_MAIL, mail).build());
try (Connection c = dataSource.getConnection()) {
    c.setAutoCommit(false);
    // ... your own writes on c ...
    postgresStore.enqueue(c, record);
    c.commit();
}
```

An insert with the same unique key as another open transaction waits until that transaction ends. Enqueue
last, after taking your own row locks, or outside the transaction, to avoid a lock-order cycle.
`SqliteTaskStore.enqueue(Connection, TaskRecord)` works the same way; see "SQLite" for its locking.

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

Ids must be allocated by a client of this namespace: `client.nextId()` or an exception id outside its
`notStored()` positions. An id that was never allocated is rejected with a `TaskValidationException`. An id
that already belongs to a task is accepted again only for the same request, encoded identically: the stored task is compared on type, queue, the
payload as stored text (when the task still has it; a finished task may have dropped it) and the unique key
(only while the stored task holds one, even after an operator retry). Headers, account, attempts, retry policy,
`startBy`, `maxRunTime`, delay, `at(...)`, the `keepFor` duration and payload version are not compared. Any other
difference throws `TaskIdReusedException`. Before writing,
`enqueueAll` rejects duplicate caller ids, checks the high-water mark before reserving fresh ids, rejects caller
ids inside that fresh range, and compares every caller id with its stored task. These checks add no store reads
for requests without caller ids. Caller ids below the end of this client's own allocated block also skip
the high-water read. A Redis failover that rolls the id counter back can make an allocated id look
unallocated.

With an existing id and matching content, `Accepted` only confirms that the record exists; it never starts
another run, even when the task is DEAD. Use `AdminOps.retry` to run a dead task again.

An empty `EnqueueFailedException.ids()` means nothing was written by this call: resend the **same requests
unchanged**, keeping any ids you set yourself. For a batch, failure during the high-water read or initial
reservation returns empty ids; after reservation, failures carry every request's last assigned id in request
order. A single `enqueue` retains its caller id even if the high-water read fails, and carries a generated id
only once it tried to write it.

For a failed batch, `settled()` holds the results of the chunks that completed, and `retryRequests`, called with
the list that just failed in the same order, returns the requests after them. The settled requests are not sent
again, so a settled `Duplicate` cannot start a task once its key's holder has finished. If the retry fails too,
prepare the next one from the retry list, not the original, or the ids of tasks the first call may have stored are
lost and those tasks are stored twice. The batch's results are the settled ones followed by the retry's:

```java
var results = new ArrayList<EnqueueResult>();
List<? extends TaskRequest<?>> batch = requests;
for (int attempt = 1; ; attempt++) {
    try {
        results.addAll(client.enqueueAll(batch));
        break;
    } catch (EnqueueFailedException e) {
        if (attempt == 3) throw e;
        results.addAll(e.settled());
        batch = e.retryRequests(batch);
    }
}
// results has one entry per request, in order
```

`notStored()` contains zero-based positions of generated-id requests this call knows it did not write:
confirmed collisions and requests in unattempted chunks, including failures before the first write.
**Do not reuse the ids at those positions:** they may belong to other tasks, even with identical content.
`retryRequests` leaves those requests without an id so the client generates fresh ids. It sets the last
assigned id on other positions, including writes whose replies were lost, and always preserves caller-set
ids. With empty `ids()`, it returns every request unchanged. Retries must encode identically and prevent
duplicates only while the original tasks are retained; after purge, the same allocated id stores a new task.
One case stays open: when a write's reply is lost, the client cannot tell whether its generated id was free, so a
retry keeps that id; if another task with identical content already held it, the retry answers accepted for that
task and this one is not stored.

Generated ids answered "exists" abandon the client's current id block and are retried from a newly allocated
block, at most three writes per request. On exhaustion, replacement-allocation failure or replacement overlap
with a caller id, the colliding positions remain marked in `notStored()`; exhaustion does not reserve more
replacement ids. A caller id taken by another writer after preflight also fails with `EnqueueFailedException`,
carrying a `TaskIdReusedException` cause. Such caller-id reuse still requires caller correction.

On PostgreSQL the id check reads the id sequence with `pg_sequence_last_value`, which
needs only the USAGE privilege on `<prefix>_task_id` that `allocateIds` already uses.

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
also has its thread interrupted once per attempt, so sleeps and blocking calls wake up; the exception that follows is treated
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

While a cancel is requested, any exception the processor throws ends the task CANCELLED, also a
`TaskUnrecoverableException` or a `TaskRescheduleException`, so a body that reschedules itself every attempt
can still be cancelled. A request that reaches the store after the last renewal of an attempt that then fails
stays on the task when it is retried, so the next attempt starts with `cancelRequested()` true.

`interruptOnCancel()` also interrupts the body when lease renewal reports a lost lease. Only an attempt
actually interrupted for lease loss has its outcome discarded, even if it swallows the interrupt and returns.
Without that interrupt, a returned result or failure still goes through the store's revision fence and can
be applied before maintenance takes over. `checkCancelled()` throws `TaskLeaseLostException` on known lease
loss. A discarded attempt emits `Retrying` with outcome `LOST`, without changing the stored task.
Interruption is opt-in because it can close sockets and NIO channels used by the body, including pooled
database or Redis connections. Processors may run on virtual threads, where blocking socket I/O is interruptible.

Starting a new `ctx.step` also checks. If the revision fence still accepts it, a processor that ignores cancellation
and returns normally ends the task SUCCEEDED; the request is not applied after a normal return. If the worker dies while a cancel is pending, maintenance
ends the task CANCELLED when its lease expires instead of retrying it.

`maxRunTime(duration)` uses the same path: when an attempt has run longer, the lease renewal asks it to stop with
the reason "max run time ... exceeded". A task without its own `maxRunTime` gets the runner's
`defaultMaxRunTime(duration)`, one hour unless the builder sets another; a task that legitimately runs longer
needs its own `maxRunTime` (at most `TaskRequest.MAX_RUN_TIME`, about 24.8 days). An attempt that is still running one
renewal period after the request is abandoned: the runner stops renewing its lease, so the lease expires and
maintenance retries or ends the task, and raises the issue `attempt-abandoned`. The abandoned thread keeps its
executor and queue slots until the processor returns, which `RunnerHealth.abandonedAttempts` and the gauge
`bth4j.runner.abandoned` count. A processor that blocks without a timeout therefore costs a slot, not the
task. An attempt whose processor has returned keeps its lease renewed until its outcome is written and is not
asked to stop or abandoned meanwhile, even when a slow store write takes it past its maximum run time.
`startBy(instant)` expires a task that did not start in time:
claims skip it, maintenance moves it to EXPIRED and the observer receives `TaskEvent.Expired`.

```java
client.enqueue(TaskRequest.of(EXPORT, export)
        .startBy(Instant.now().plus(Duration.ofMinutes(10)))   // expired, not run, if no worker was free by then
        .maxRunTime(Duration.ofMinutes(30))                        // asked to stop after 30 minutes
        .build());
```

### Recurring tasks

`client.putSchedule(ScheduleDefinition.of(id, type, payload, cron))` creates or replaces a definition and
returns the next fire time; `.queue(...)`, `.account(...)`, `.maxAttempts(...)` and `.maxRunTime(...)` refine it. The cron
expression has 5 fields (Unix format). Each fire time creates one task, exactly once across all instances.
Registering the same id with the same timing (expression, zone and delay) keeps the next fire time while a
fixed-delay occurrence is still running, or while it is not beyond the second fire after now; changed timing, or a
next fire time beyond that with no occurrence running (a parked or pushed-out definition), resets it. Registering a
definition that is stored already, with its next fire time kept, writes nothing and logs at DEBUG; a new or
changed one logs at INFO. `client.deleteSchedule(id)` and `client.schedules()` remove and list definitions.

More options on `ScheduleDefinition`:

```java
// cron in a time zone (UTC by default); a fire time that does not exist in a DST gap moves to the next valid
// instant, a repeated hour fires once
client.putSchedule(ScheduleDefinition.of("daily-digest", DIGEST, "0 8 * * *")
        .zone(ZoneId.of("Europe/Berlin")));

// misfire: what happens when a fire time was missed by more than the threshold (below)
client.putSchedule(ScheduleDefinition.of("hourly-sync", SYNC, "0 * * * *")
        .misfire(Misfire.catchUp(3)));          // Misfire.SKIP, Misfire.FIRE_ONCE_NOW (default), Misfire.catchUp(maxFires)

// overlap: no new occurrence while the previous one has not finished
client.putSchedule(ScheduleDefinition.of("nightly-report", NIGHTLY_REPORT, "30 2 * * *")
        .overlap(Overlap.SKIP));

// fixed delay: the next occurrence is due one delay after the previous one succeeded
client.putSchedule(ScheduleDefinition.every("poll-feed", POLL_FEED, Duration.ofMinutes(5)));
```

The misfire threshold is twice `recurringTasksCheckInterval`, at least one minute. `SKIP` creates nothing and
continues from now, `FIRE_ONCE_NOW` creates one task now, `CATCH_UP` creates one task per missed fire time up
to `maxFires` (the most recent ones when more were missed) and continues from now. After a downtime of more than
`maxFires` fires, `CATCH_UP` moves in one step to the last `maxFires` missed fires and creates them from that
check on, up to about 100 per check, including those late by less than the threshold; beyond 100,000 missed fires it creates one task now, as `FIRE_ONCE_NOW` does. Every misfire is recorded as an
issue. With `Overlap.SKIP`, an occurrence is skipped (and an issue `schedule-overlap`
recorded) while the task of the previous one is not finished. The check creates an occurrence up to one
`recurringTasksCheckInterval` before its fire time and decides the overlap then. This keys only the occurrence (`sched:<id>`); tasks its processor
enqueues are unique only if the application gives them keys. A fixed-delay definition (at least 1 second)
first fires one delay after registration; if an occurrence ends DEAD or CANCELLED (in its processor, through
an expired lease or by an operator cancel) the definition is paused and an issue `schedule-paused` is recorded, so a failing job does
not run again until an operator resumes it. Pausing,
resuming and triggering a definition are `AdminOps` operations; a paused cron definition keeps its stored next
fire time. Resuming a definition that is not paused changes nothing. Triggering moves the next fire time to now;
the task is created by the next recurring check, and a paused definition creates nothing until it is resumed
(resuming a fixed-delay definition sets its next fire one delay from now). A trigger also forgets the occurrence
in flight, so when that task ends it neither re-arms nor pauses the definition. A definition that cannot be
evaluated (an invalid cron expression, a time zone the JVM no longer knows) has its next fire moved ahead by at
least an hour, or twice the check interval, with a `schedule-broken` issue each time, so it does not hold back
the others.

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
use it. All queues' shares and caps go to the store in one atomic claim per cycle. The store first tries
each share in order, then offers unused slots to queues that filled their share, respecting each cap.
Queues without a policy have weight 1 and no limit. Limits are per runner, not per fleet. When a claim
found nothing because a queue was at its cap, the runner claims again as soon as a task of a capped queue
finishes. This wake-up survives a processor restart; `idlePollInterval` remains the timeout fallback.

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
The runner ends the task as dead anyway when the attempts are used up (10 by default, at most 32767).
For "retry until it works", throw `TaskRescheduleException(delay)` for expected transient failures: it runs
again later without spending an attempt. Set a high `maxAttempts` with a capped backoff for crashes; the
attempt budget is still finite.

A retry's due time is computed on the worker's clock; promotion and leases use the store's clock. Keep
these clocks synchronized: skew changes the effective retry delay. After a failover to a store server whose
clock is behind, due tasks wait and leases last longer by that difference.

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
after the outcome was applied, and an exception in it never changes a task. Events that end an attempt carry an
`outcome`: `APPLIED` (the store made the change), `LOST` (the store rejected it because the lease had moved on) or
`UNKNOWN` (the store call failed, so the change may or may not have been made; if it was not, the lease expires and
the task runs again):

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
puts `bth4j.task`, `bth4j.type` and `bth4j.queue` in the MDC; pass your own `Logger` to use another name.

The runner's own log lines go to the logger names in `Loggers`: `RUNNER`, `CLIENT`, `PROCESSOR`, `ATTEMPT`
(one attempt of a task), `MAINTENANCE`, `RECURRING`, `PROMOTION`, `EXECUTOR` (`DefaultVtExecutor`) and
`OBSERVERS`, plus `TASKS` for `LoggingTaskObserver`. They follow the class names below `Loggers.ROOT` (`today.bonfire.oss.bth4j`), so a
level set on that prefix covers them all except `TASKS`, and they do not change when an internal class moves.

### Inspection

```java
client.task(id);                                  // Optional<TaskView>, any state
client.counts();                                  // per queue name: counts per state
TaskViewPage page = client.tasks(TaskState.DEAD, "mail", null, 100);   // oldest state change first (QUEUED and SCHEDULED: earliest due); null cursor starts
client.tasks(TaskState.DEAD, "mail", page.next(), 100);   // next is null after the last page
```

The listing returns a `TaskViewPage` whose `next` is an opaque position in the listing, so the walk does not depend on
the last task of the previous page. A task whose own position moves while the walk runs (for example a deferral) can
still be seen twice (moved later) or missed (moved earlier). A malformed cursor throws `IllegalArgumentException`.
A task whose stored value cannot be read is left out, so a page can hold fewer tasks than asked for while `next`
is not null; `counts()` still counts it.

A `TaskView` carries state, attempts, due time, the last failures, completed steps and progress.

### Health and issues

```java
RunnerHealth health = runner.health();     // status NEW, RUNNING, QUIET or STOPPED; workerId, runningTasks,
                                           // abandonedAttempts, lastClaimAt, lastMaintenanceAt, lastStoreErrorAt,
                                           // lastStoreError, issueCount
List<Issue> issues = runner.issues();      // the last 100 things an operator should look at, oldest first
boolean drained = runner.drain(Duration.ofSeconds(30));   // quiet, then wait for the running tasks
```

An `Issue` has a time, a kind, a detail and the task id when there is one. The kinds are `unknown-type`,
`store-error`, `lease-lost`, `attempt-abandoned`, `retry-policy`, `executor-rejected`, `schedule-misfire`,
`schedule-overlap`, `schedule-paused`, `schedule-rearm`, `schedule-broken`, `corrupt-task` and
`resource-signal-unavailable`. `schedule-rearm` means a fixed-delay definition was not re-armed because its
delay header cannot be read; `schedule-broken` is described under "Recurring tasks". Repeats of a `store-error`
with the same text are one issue whose detail carries the count and whose time is the latest. A `corrupt-task` is a
task with a stored value the store cannot read, such as a hand-edited row: a claim leaves it out and ends its
lease, and maintenance ends it DEAD with the failure `unreadable-record` within one check interval; the purge
removes it after `deadRetention` like any dead task. Listings leave it out, and reading it answers
`UnreadableTaskException` (422 over the admin HTTP API). `AdminOps.cancel` and `delete` still work on it given
its id and revision; a corrupt fixed-delay occurrence is then not treated as one, so the cancel or delete
neither pauses nor re-arms its definition. Issues live in the memory of the runner that saw them. `drain` leaves the runner quiet
(call `resume()` to claim again, or `stop()`), and returns whether every running task finished in time; use it
before a rolling restart. It returns true only when no claim that started before the runner went quiet is
still in progress and no attempt is unfinished.

Each runner also writes a heartbeat (last seen time, running tasks and whether it is accepting work) to
the worker registry every `maintenanceCheckInterval`. New workers accept work. `quiet()` and `drain()`
stop accepting; `resume()` accepts again. Quiet and resume request an immediate heartbeat on the
maintenance thread without blocking the caller on the store. Maintenance deletes workers that were
silent for `workerStaleAfter` (10 minutes by default). `admin.workers()` lists them, including `accepting`,
the queue ids they process and their registered task type ids, recorded at startup. A runner that is still
alive, for example one that could not reach the store for longer than `workerStaleAfter`, writes its row again
with its next heartbeat, under the same id and with its label, queues and types; the row's start time is then
the time it was written again.

`admin.unservedWork()` finds queued work that no worker seen within the last minute can process;
`admin.unservedWork(Duration)` chooses another heartbeat window using the admin clock. Paused queues
are skipped, and workers that are not accepting do not contribute coverage. A queue without a live
accepting worker produces `UnservedWork(queue, null, waiting)` with its full queued count. For queues
with live accepting workers, each unhandled type produces `UnservedWork(queue, type, waiting)` with
its exact queued count. Both come from `InspectionStore.queuedByType()` at the time of that read.
A worker's last seen time is the store's clock, and the window is measured on the admin clock, so a skew
between them shifts the window by that much.

### Admin API

`AdminOps` is the operator's view and set of operations over one store. `runner.admin()` also reports that
runner's health and issues; `client.admin()` works in a process without a runner, where `health()` is empty and
`issues()` is an empty list.

```java
AdminOps admin = runner.admin();

admin.counts();                                    // Map<queue name, StateCounts>
admin.queues();                                    // id, name, paused
admin.tasks(TaskState.DEAD, "mail", cursor, 100);  // oldest state change first, QUEUED and SCHEDULED earliest due; TaskViewPage(items, next); cursor null starts, next null ends; queue null for all
admin.task(id);                                    // Optional<TaskView>
admin.taskByKey("welcome-42");                     // the task holding that unique key, Optional<TaskView>
admin.retry(id, revision);                         // DEAD, CANCELLED or EXPIRED back to QUEUED, attempts reset
admin.cancel(id, revision, "reason");              // CANCELLED, REQUESTED or STALE
admin.cancelByKey("welcome-42", "reason");         // the holder of the key: CANCELLED, REQUESTED, STALE (changed meanwhile), NOT_FOUND (also a removed holder) or FINISHED
admin.delete(id, revision);                        // any state but LEASED
admin.pauseQueue("mail", true);
admin.schedules();                                 // ScheduleView: cron, zone, misfire, fixed delay, paused, next fire
admin.pauseSchedule("poll-feed", false);           // resume a paused one; a fixed-delay definition is re-armed one delay from now
admin.triggerSchedule("nightly-report");           // fires at the next recurring check
admin.deleteSchedule("nightly-report");
admin.workers();
admin.unservedWork();
admin.purgeStaleWorkers(Duration.ofMinutes(10));
```

Every change to a task takes the revision the operator saw. If the task changed meanwhile, nothing is done and
the call returns `false` or `STALE`. `AdminOps.retry` returns `RetryOutcome.RETRIED` on success and
`RetryOutcome.STALE` for a missing task or a revision or state mismatch. A retried task keeps its failures and
steps, resets its attempts to zero, and loses its `startBy`, so a retried expired task does not expire again.
Inside a `keepFor` window it retains its unique key without expiry while open; `taskByKey` and `cancelByKey`
still reach it, and a same-key enqueue answers `Duplicate` with its id. The `keepFor` setting is kept, so a
fresh keep window starts when the retried task finishes. If another task holds the retained key, retry returns
`RetryOutcome.DUPLICATE` and changes nothing. The admin HTTP route answers 409 with
`{"code":"KEY_HELD","error":"the unique key is held by another task"}`; the dashboard matches the code.

A default until-done key is released when the task finishes; a `keepFor` key is released when its window
passes. Retrying after either release runs the task **without a key**. If the key is free, an enqueue with that
key is accepted and starts a second task. If another task has taken it, that task remains the holder.

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
| `bth4j.tasks` | counter | `type`, `queue`, `result` (`succeeded`, `retried`, `died`, `rescheduled`, `cancelled`, `expired`, `lease_lost`), `outcome` (`applied`, `lost`, `unknown`) |
| `bth4j.task.duration` | timer | `type`, `queue` |
| `bth4j.deferred` | counter | `type` |
| `bth4j.schedule.fired` | counter | none |
| `bth4j.purged` | counter | `state` |
| `bth4j.runner.running` | gauge | none; includes tasks whose outcome is still being written, up to twice the concurrency limit with the built-in executors |
| `bth4j.runner.abandoned` | gauge | none; attempts past their maximum run time that still hold a slot |
| `bth4j.runner.up` | gauge | none; 1 while the runner is RUNNING |
| `bth4j.tasks.state` | gauge | `state`, `queue` |

The observer's tags are only names and results, never ids or messages, so the number of meters stays bounded.
`expired` and `lease_lost` are counted with `type` and `queue` set to `unknown`. The gauges are refreshed by
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
(`sessionTtl`) and end when the server stops. A user has at most 20 sessions; a login past that ends that
user's oldest session. Every fifth failed login from a remote address blocks it for 10 seconds (HTTP 429); a
successful login does not clear the failures counted before it. A stale revision is answered 409, an unknown id
404, and a task whose stored value cannot be read 422 with `code: "UNREADABLE_TASK"`; task ids and revisions are
JSON strings.
The endpoints are `me`, `session`, `health`, `overview`, `queues` (with `pause` and `resume`), `tasks` (list
with `state`, `queue`, `cursor` and `limit` up to 200; one task, its `payload`, `retry`, `cancel`, delete;
`tasks/by-key/{key}` reads the task that holds a unique key, 404 when none does, and `tasks/by-key/{key}/cancel` cancels it
with a body of `{"reason"}` and no revision: viewers may read, admins may cancel; a blank key or one over 256 characters
is answered 400; cancel answers 404 when no task holds the key and 409 when the holder is already finished or changed meanwhile),
`schedules` (with `pause`, `resume`, `trigger`, delete), `workers`, `unserved` and `issues`.

`GET /api/v1/workers` includes `queues` and `types` as name lists and the boolean `accepting`. `GET /api/v1/unserved` returns a list
of `{"queue", "type", "waiting"}` using the one-minute heartbeat window. A null type means the whole queue is unserved and waiting is its queued
count; otherwise waiting is the exact queued count of that type. Both endpoints are available to viewers.
The dashboard shows coverage and quiet status on Workers and unserved work on Overview.

`GET /api/v1/tasks` answers `{"items": [...], "next": "<cursor>" | null}`: pass `next` as `cursor` for the following
page; `next` is null after the last page. A missing or empty `cursor` starts at the beginning, and a malformed one is
answered 400.

The server speaks plain HTTP unless `https(SSLContext)` is set, which also marks the cookie `Secure`; keep it
on the loopback interface or behind TLS. Behind a TLS-terminating proxy the server itself speaks plain HTTP, so set
`secureCookie(true)` to mark the cookie `Secure`. Every authenticated response renews the cookie's `Max-Age`, so
the browser's expiry follows the sliding session expiry. A server runs once: `start()` after `stop()` throws, so
build a new one. An unknown queue name is answered 400 in the tasks filter and 404 on pause and resume. `corsOrigin("https://dashboard.example.com")` lets a browser app on
another origin call the API with credentials.

The dashboard is the static Vue 3 app in `dashboard/` (no build step; it ships its pinned Vue runtime in
`dashboard/vendor/` and loads nothing from the network). It shows overview, queues, tasks with filters and detail
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
`Duration.ZERO` removes the task at the transition instead. Retention and the key window are independent:
retention decides how long the task stays readable, `keepFor` how long its key blocks a same-key enqueue. A task
removed inside its window (retention zero at the transition, or purged after its retention) is gone from every
read, while its key still answers a same-key enqueue with `Duplicate` and the removed task's id until the window
ends.

### Concurrency

By default a runner runs twice as many tasks at once as there are processors, each on a virtual thread.
`concurrency(Concurrency.fixed(n))` sets another limit. Adaptive concurrency starts at `min` and stays within
`[min, max]`, targeting whole-machine CPU and memory usage:

```java
Concurrency.adaptive(8, 256) // CPU 0.90, memory 0.80, heap only
Concurrency.adaptive(8, 256, 0.75, 0.70).withMemorySignal(MemorySignal.HEAP_AND_SYSTEM)
```

A daemon samples every 500 ms with fixed delay and monotonic time, without catch-up after a pause. CPU uses
`OperatingSystemMXBean.getCpuLoad()` (container-aware on JDK 25), smoothed with EWMA alpha 0.25, without a
load-average fallback. Heap usage is used / max (committed if max is undefined), taking the lowest reading
in the last 2 seconds to smooth the GC sawtooth. `HEAP_AND_SYSTEM` takes the greater of heap and system
usage, each with its own 2-second minimum. Reductions use the highest valid component, so an unavailable
system reading cannot hide heap pressure; growth requires every configured component to be valid. System
usage catches native memory such as image buffers, but can count OS file cache as used and hold the limit
low on a long-running host.

A claim that fills its executor reservation provides demand evidence for 5 seconds and allows one growth
step, so the slots a step adds must be filled before the next one; a partial or empty claim clears it. Growth requires saturation: running plus reserved slots must be at least the current
limit, including permit debt after a reduction. With fresh evidence, raw and smoothed CPU and memory at
least 0.15 below their targets grow the limit by 25% (rounded up, at most 16 slots); at least 0.05 below
grows by one. Without fresh evidence, a saturated runner with at least 0.05 headroom probes by adding
exactly one slot, at most once every 5 seconds. A full claim restores demand evidence; an empty claim
leaves the slot free, stopping growth. Probes obey the same resource checks and cooldowns as other growth.
When queue caps (`maxConcurrent`) keep claims from filling the reservation while work waits, the claims are
partial, so the limit grows only by these one-slot probes.
For targets below 0.2 these margins scale proportionally. Each tick makes at most one increase; free slots
always prevent growth.

Memory at or above 0.95 resets the limit to `min`. Memory at its target for 1 second reduces the limit by
25%, at most once per second, and blocks growth for 5 seconds. Smoothed CPU above its target plus 0.03 for
1 second reduces it by 20%, at most once per second, and blocks growth for 2 seconds. Memory takes priority.
Invalid or unavailable CPU or any configured memory component blocks growth; after 5 seconds without a
valid reading for that signal, the limit decays by 25% per second toward `min`. Two fresh valid samples of
each configured signal are needed before growth resumes. The first time a signal goes 5 seconds without a
valid reading, the executor logs a WARN and the runner raises the issue `resource-signal-unavailable`; a JVM or
container that never provides the reading keeps the limit at `min`, where `Concurrency.fixed` fits better. After 30 seconds without a running task seen
at a sampling tick or a nonempty claim, the limit returns to `min`. Reservations, releases and empty polls
do not restart that idle timer.

Shrinking never cancels tasks or blocks waiting for them: permits may remain negative until occupied slots
finish; saturated growth and probes can recover even while those tasks keep running.
`runner.concurrencyLimit()` exposes the current limit when supported by the executor. Custom executors can
receive demand through `BackgroundExecutor.claimed(reserved, claimed)` and expose their limit through
`limit()`; both have defaults.
Limit changes are logged at DEBUG and holds at TRACE under `Loggers.EXECUTOR`.

## Configuration

All settings are on `BackgroundRunner.builder()`.

| Setting | Default | Meaning |
|---|---|---|
| `store` | required | `new RedisTaskStore(jedis, "namespace")`, `new PostgresTaskStore(dataSource, "prefix")` or `new InMemoryStore()` |
| `registry` | new registry | the processor registry; by default created by the runner and reached with `runner.registry()` |
| `jsonMapper` | none | payloads and step results as JSON; needed unless every task type has its own codec |
| `clock` | `Clock.systemUTC()` | clock for client-side instants (`createdAt`, delays, retry times) |
| `concurrency` | `Concurrency.fixed(max(2, 2 x processors))` | how many tasks run at once; `fixed(n)` or `adaptive(min, max[, cpuTarget, memoryTarget])`; adaptive defaults: CPU 0.90, memory 0.80, `HEAP` |
| `taskExecutor` | built from `concurrency` | a custom `BackgroundExecutor`; the runner shuts it down |
| `retryPolicy` | exponential, 1 s doubling, capped at 1 h, 20 % jitter | delay after a failed attempt |
| `retryPolicy(name, policy)` | none | adds a named policy that requests choose with `TaskRequest.retryPolicy(name)`; name 1 to 64 characters; may be called several times |
| `queue(name, QueuePolicy)` | `QueuePolicy.DEFAULT` (weight 1, no limit) | share of free slots and concurrency limit of a queue on this runner |
| `observer` | none | receives every `TaskEvent` |
| `failureDetail` | `FailureDetail.FULL` | `FULL` records the class name, message and stack of a failure; `CLASS_NAME_ONLY` keeps only the class name, the attempt and the time. The stored failure, the `TaskEvent` error (a `RedactedFailure` that has the class name as `toString()`), the runner issues and the runner's log lines then carry no message and no stack. A rethrown `VirtualMachineError` reaches the thread's uncaught exception handler with its message and stack, so `CLASS_NAME_ONLY` does not cover that output. The message of a `TaskCancelledException` is not stored as the cancel reason; the operator's reason or "cancel requested" is. Progress text and messages that your own retry policy returns are not touched. Application text is not covered: an operator's cancel reason, `ctx.progress` text and a `RetryPolicy.Decision.dead(reason)` reason are stored, shown by the admin API and logged as given, so hosts must keep personal data out of them |
| `workerLabel` | JVM name (`pid@host`) | identifies this process in the worker registry; 1 to 128 characters |
| `threadGroupName` | `bgr` | name of the runner's thread group |
| `succeededRetention` | 10 minutes | how long succeeded tasks are kept; zero removes them at once |
| `deadRetention` | 90 days | how long dead tasks are kept; zero removes them at once |
| `cancelledRetention` | 48 hours | how long cancelled and expired tasks are kept |
| `keepSucceededPayload` | false | keep the payload of succeeded tasks (dropped on success by default) |
| `purgeInterval` | 1 minute | time between purge runs |
| `purgeBatchSize` | 500 | tasks removed per purge call; the purge loops until a call removes fewer |
| `leaseDuration` | 60 s (at least 1 s) | how long a claimed task stays leased without renewal; renewed every third while running. A lapsed lease cannot be renewed any more, so keep it well above the worst pause (GC, container freeze, slow store round trip); a few seconds is fragile |
| `idlePollInterval` | 500 ms (at least 10 ms) | sleep after a claim that found nothing (cut short when a queue at its `maxConcurrent` frees a slot), and the wait for a free executor slot |
| `claimBatchSize` | 32 | most tasks claimed in one call, bounded by free executor slots |
| `shutdownTimeout` | 30 s | how long `stop()` takes at most, waiting for services and running tasks |
| `serviceCheckInterval` | 10 s | how often a dead service thread is restarted |
| `maintenanceCheckInterval` | 5 s (at least 1 s) | how often expired leases are looked for, tasks past `startBy` are expired and the worker heartbeat is written |
| `workerStaleAfter` | 10 minutes | worker registry rows without a heartbeat for this long are deleted by the purge |
| `scheduledTasksCheckInterval` | 1 s (at least 10 ms) | upper bound of the wait between promotions of due scheduled tasks; it wakes earlier for the next due task |
| `recurringTasksCheckInterval` | 10 s | how often recurring definitions are checked; twice this value (at least 1 minute) is the misfire threshold |
| `idBlockSize` | 64 (1 to 100000) | task ids the client reserves per store call; prefer more than 1 for busy producers (see below) |
| `defaultMaxAttempts` | 10 (1 to 32767) | attempts for requests that do not set `maxAttempts` |
| `configureQueues` | `Q:DEF`, `Q:LOW`, `Q:MED`, `Q:HIGH`; default queue `Q:DEF` | available queues, queues this instance processes (round robin), default queue |

With `idBlockSize(1)` every enqueue reserves its id with a store call, and on PostgreSQL that call takes the
lock that keeps block allocation contiguous, so busy producers serialise on it. In a contention measurement on
PostgreSQL, size 1 reached about one third of the enqueue rate of a larger block (10,743 against 32,379
enqueues in 60 s).

`start()` fails with `TaskConfigurationException` when no processor is registered. A `start()` that fails later,
for example because the store cannot be reached, stops what it started and can be called again. `quiet()` stops
claiming while running tasks finish and `resume()` claims again. `stop()` stops the services, waits up to
`shutdownTimeout` in total, also when a service is stuck in a store call, and keeps renewing leases of running
tasks meanwhile; tasks still running afterwards are retried when their lease expires. A lease that has lapsed is not revived by a later renewal, so `leaseDuration` must sit well above the worst pause of a worker (GC, container freeze, slow store round trip).

A runner acknowledges successful tasks in batches without a timer: a task that finishes while no batch is on its
way is acknowledged at once, and tasks that finish while one is on its way go together in the next
(`ExecutionStore.ackAll`, one script on Redis, one statement on PostgreSQL). A succeeded occurrence of a
fixed-delay schedule keeps its own call, because its re-arm is part of the same write. A task's executor slot is freed
when its processor returns, so the slot takes the next task while the outcome is written; the task keeps its lease
renewed until the store answers, and its queue's `maxConcurrent` counts it until then. The built-in executors let at
most twice their current limit run or write their outcome at once, so a slow store holds back claims; adaptive
concurrency counts the slots this bound holds back as occupied. `runningTasks` and `bth4j.runner.running`
count a task until its outcome is written, so they can reach twice the limit. Observers of outcome events run after
the slot is freed. A custom `BackgroundExecutor` keeps the slot until
the task ends unless it overrides `executeWithSlot`. A process that only produces tasks can use `TaskClient.builder().store(store).jsonMapper(mapper).build()`.

## Stores

### Redis

```java
var store = new RedisTaskStore(jedis, "my-app");   // UnifiedJedis (for example a RedisClient) and a namespace
```

The namespace prefixes every key and carries the Redis Cluster hash tag, so all keys of a store share one
slot. Redis Cluster is not tested. The scripts declare only one key and derive the others, so the namespace's slot
must not be resharded while runners use it: during a slot migration a script can fail after part of its writes.
The store does not close the Jedis client. Redis 7.4 or newer, or Valkey 9 or newer, is required (the
store expires the key fields of finished tasks with hash field expiry). A namespace written by bth4j 3.0 has
another layout and is refused by `init`, and so is one written by 4.0.0, a 4.0.0 snapshot or an earlier 4.1.0
snapshot (any layout but 5); start with a new namespace. Enqueue writes a random per-record
token in task hash field `wt` with the other fields. A transport resend with that token returns stored again;
a different token under the same id returns exists, before checking the unique key. The token only helps while
the task exists: with a zero succeeded retention a task that already ran and was removed is stored again by a
resend.

Layout 5 stores queued tasks in `q:<queue-id>:<type-id>` zsets scored by due milliseconds, and keeps in
`active:<queue-id>` the set of types whose zset in that queue is not empty. A runner's known types are stored
once as the set `ts:<digest>`, named by a digest of the sorted type ids and shared by runners with the same
types; it expires a day after the last claim that named it and a claim writes it again when it is missing. A
claim takes the intersection of the two sets, so its cost grows with the types that have queued tasks, not
with the types registered. A claim runs both grant passes in one atomic script and merges the eligible types
by `(due, lexical id)`, leaving foreign tasks unchanged. Its scan budget is `min(count * 4, 1000)` eligible
entries per queue across both passes. Queue listings merge the active type zsets by `(due, numeric id)` and
retain their position cursors when a type empties. `queuedByType()` and the counts read exact zset
cardinalities of the active types in one script without task hash reads. Finished tasks are listed and purged
from one zset per state and queue, `idx:<state>:<queue-id>`. A task removed while its unique key window is open
leaves only the key string `k:<key>`, which expires at the end of the window. Enqueue batches and acknowledgement
batches run as one script per group of tasks. A claim leaves a task whose `startBy` has passed in its queue for
maintenance to expire. Type names use `reg:type` and `seq:type`; task and
recurring-definition hashes store the type id in `ty`. Worker registration sets `accepting` to 1, and one
heartbeat script atomically updates the worker's time, running count and accepting flag, and writes the hash of
a removed worker again with its label, queues and types.

Memory: the task hash stays a listpack only while every value is at most `hash-max-listpack-value` bytes
(default 64). A header map, account, unique key, progress text or cancel reason longer than that makes the
hash a hashtable, about 5 times larger for that task; raise the setting if your tasks carry such values.
Zsets (queues, `leased`, the state indexes) leave the listpack encoding above `zset-max-listpack-entries`
entries (default 128) and cost about 6 times more per entry as a skiplist; raising it trades CPU for memory.

### PostgreSQL

```java
var store = new PostgresTaskStore(dataSource, "bth");   // tables bth_task, bth_queue, bth_type, bth_worker, bth_schedule, bth_meta
```

The tables are created when the runner or client is built, or run `store.schemaStatements()` from your
migration tool. The prefix is lower case letters, digits and underscores, at most 41 characters; runners with
the same prefix on the same database share the work. There are no foreign keys. PostgreSQL 16 or newer is
supported, 17 or newer recommended; the tests run on 18. The store runs at the connection's isolation level
and is written for READ COMMITTED, PostgreSQL's default: under REPEATABLE READ or
SERIALIZABLE, concurrent claims and fenced writes fail with serialization errors. Tables written by bth4j 3.0 or 4.0.0 (schema
version 1 or 2) are refused by `init`; start with a new prefix or drop the tables.

Schema version 3 stores task types in `<prefix>_type` and `type_id` columns in the task and schedule
tables. Worker coverage uses `queue_ids` and `type_ids` as `smallint[] NOT NULL`; `accepting` is a
required boolean, true on registration and updated by heartbeat.

`init` takes a transaction-level advisory lock for the prefix, then only reads the stored schema version when the
tables exist; it runs DDL only when there is no schema yet. A runner role on existing tables needs only data
privileges (SELECT, INSERT, UPDATE, DELETE on the tables, USAGE on the sequences), and starting a runner takes no
table lock. `payload_version` and `catch_up_max` are `integer`, so every positive Java `int` is stored. A task
removed inside its unique key's keep window leaves a key-only row (state code 7, content columns empty) in the
task table until the window ends; it is not counted or listed.

The store sets no query timeout of its own. Configure them on the driver and pool: for pgjdbc `socketTimeout`
and `tcpKeepAlive`, and a `statement_timeout` (for example `options=-c statement_timeout=30s`). Without them, a
half-open connection can block a runner thread for as long as the operating system keeps the socket, while
health still reports RUNNING. A write whose reply was lost to a timeout may or may not have committed; the
runner treats it as `UNKNOWN` (see "Observing a runner").

Queue grants are claimed in one statement that runs both passes, each with a bounded per-queue
`FOR UPDATE SKIP LOCKED` scan, so a claim is atomic without a transaction.
`queuedByType()` uses one `GROUP BY queue_id, type_id` over queued
rows. The queued partial index ends with `type_id` to cover those counts while preserving claim order;
it uses ordinary index columns. `counts()` reads the `<prefix>_task_counts` index (`queue_id, state`), listing
one finished state of one queue reads `<prefix>_task_finished_queue`, and listing QUEUED across all queues scans
the queued index once per queue and merges by due time and id. A claim walks the queued tasks of a queue in due
order past those of types it does not handle, so a large backlog of a type no live runner handles makes every
claim on that queue slower; give such types their own queue.

### SQLite

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

The store is for one or more runners in one process or in several processes on one machine, with the database
file on a local disk. Network file systems are not supported: SQLite's WAL mode does not work over them, and the
store does not detect them. For runners on several machines use PostgreSQL or Redis.

The tables are created when the runner or client is built, or run `store.schemaStatements()` from your
migration tool. The prefix is lower case letters, digits and underscores, at most 41 characters, and neither
`sqlite` nor starting with `sqlite_`, because SQLite reserves table names that start with `sqlite_`; runners with
the same prefix on the same database file share the work. All tables are `STRICT`, there are no foreign keys,
and times are stored as epoch milliseconds taken from the store's `java.time.Clock` (the third constructor
argument, `Clock.systemUTC()` by default), never from an SQL function. The SQLite store has its own schema
version, 1, stored in `<prefix>_meta`; `init` refuses tables with another version and tables that have no
`<prefix>_meta`, and when the version matches it only reads. SQLite 3.45 or newer is supported, the latest
release recommended; the tests run on 3.54 (sqlite-jdbc 3.54.1.0, which carries its own SQLite). The store uses
no feature newer than 3.45 and does not check the version. On JDK 25 the JVM prints a warning when sqlite-jdbc
loads its native library, unless the application starts with `--enable-native-access=ALL-UNNAMED`.

`init` refuses to run unless `PRAGMA journal_mode` is `wal` and `PRAGMA busy_timeout` is at least 1000 on the
connection it receives, and its exception names the setting and how to set it. Without WAL a writer blocks
readers, and without a busy timeout concurrent runners fail with `SQLITE_BUSY` instead of waiting for the write
lock. Set both on every connection, as in the example, or in the JDBC URL
(`jdbc:sqlite:/path/tasks.db?journal_mode=WAL&busy_timeout=5000`). `synchronous` is your choice and is not
checked: `FULL` keeps every confirmed commit through a power cut; `NORMAL` survives a crash of the application
but can lose the last commits on power loss.

SQLite allows one writer at a time. Every write of the store runs in a `BEGIN IMMEDIATE` transaction that the
store issues itself, whatever the pool does to the connection, so a writer waits for the lock up to the busy
timeout and then fails with a `TaskStoreException`. A claim is several short statements in one such
transaction: it picks the ids for both passes of the queue grants, then leases them with one `UPDATE` that takes
the id list through `json_each`. A claim, promotion, purge or expiry that finds nothing to do reads first and
takes no write lock. The store returns every connection with the auto-commit setting it received. A pool that
turns auto-commit off together with `transaction_mode=IMMEDIATE` in the JDBC URL makes the driver begin a write
transaction on every idle pooled connection, which holds the write lock; do not combine the two. Writes
queue behind one another, so the write rate of the whole database is that of one writer.

A task removed inside its unique key's keep window leaves a key-only row (state code 7, content columns empty)
in the task table until the window ends; it is not counted or listed. The queued partial index ends with
`type_id`, and a claim walks the queued tasks of a queue in due order past those of types it does not handle,
so a large backlog of a type no live runner handles makes every claim on that queue slower; give such types
their own queue.

`SqliteTaskStore.enqueue(Connection, TaskRecord)` stores a record on your connection and does not commit; the
task exists only if your transaction commits. A write transaction holds SQLite's single write lock until it
ends, so keep it short: every other writer waits for it. A transaction that reads first and writes later can
fail with `SQLITE_BUSY` when another writer committed in between; begin it with `BEGIN IMMEDIATE`, or use
`transaction_mode=IMMEDIATE` with a pool whose connections have auto-commit on. Call `client.prepare(request)`
before your transaction begins: it can allocate ids and register queue and type names, writes on another
connection that would wait for your transaction's write lock until the busy timeout.

### Other stores

The store interface is `today.bonfire.oss.bth4j.spi.FullStore`. A store on another engine implements it and
subclasses `StoreContract` and `RunnerContract` from `bth4j-store-tests`; see "The store SPI and how to implement a store" in
[docs/design-3.0.md](docs/design-3.0.md).

### Store SPI

`ExecutionStore.claim` takes `ClaimRequest(List<QueueGrant> grants, int count, Duration lease,
int workerId, Set<Integer> knownTypeIds)`. A `QueueGrant(queueId, share, cap)` requires
`0 <= share <= cap`; grants must be non-empty and contain no duplicate queue ids. The store performs
both passes atomically across all grants, claiming each queue by `(due, id)` (Redis may compare equal-due
ids as decimal text). Pass 1 skips paused queues and zero shares, takes up to `min(share, remaining)`,
and marks a grant filled only when it took that positive amount in full. Pass 2 visits only filled grants
in order, taking up to `min(remaining, cap - alreadyClaimed)`. Total claims never exceed `count`.
Without policies each queue has `share = cap = count`, preserving queue preference order. Unknown
types stay unchanged, including their due times; a null `knownTypeIds` accepts every type.

`InspectionStore.queuedByType()` returns exact QUEUED counts as queue id to type id to count, with
only non-zero entries. `AdminStore.heartbeat(workerId, label, queueIds, typeIds, running, accepting)` persists
the worker's accepting state, exposed by `WorkerInfo.accepting()`, and creates the row again under the same id
when maintenance purged it. `allocateWorkerId` creates accepting workers.
`StoreContract` covers these behaviours for every store implementation.

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
Redis, PostgreSQL and SQLite stores, so behaviour in tests matches production. The services still wait in real
time between their checks, so a test that moves the clock should poll for the result (for example with
Awaitility) instead of asserting at once.

`new Codecs(jsonMapper)` encodes and decodes payloads (`encode(type, payload)`, `decode(type, text)`) and step
results (`encodeStep(result)`, `decodeStep(text, class)`) exactly as the runner stores them, so a test can pin
the stored form of a payload version or a step result against the library's own rules.

The integration tests of the store modules start containers with Testcontainers: `redis:8.8-alpine` and
`postgres:18-alpine` by default. `./mvnw -pl bth4j-redis -am verify -Pvalkey` runs the Redis suite against
`valkey/valkey:9.1.3-alpine`, and `-Dbth4j.redis.image=...` or `-Dbth4j.postgres.image=...` selects any
other image. The SQLite tests need no Docker: they run in the `test` phase on a database file in a temporary
directory.

Test output (logs included) goes to `target/surefire-reports` and `target/failsafe-reports` of each module, one
`*-output.txt` file per test class, so the build log shows only results and failures. Add
`-Dmaven.test.redirectTestOutputToFile=false` to see it on the console.

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
- The Redis store needs Redis 7.4 or newer or Valkey 9 or newer, the PostgreSQL store PostgreSQL 16 or
  newer (17 or newer recommended), the SQLite store SQLite 3.45 or newer (latest recommended) in WAL mode, and
  Java 21 or later is required. Redis Cluster is not tested, and a namespace's slot must not be resharded while
  runners use it.
- The SQLite store is for runners on one machine with the database file on a local disk; network file systems
  are not supported, and all writes of a database go through one writer at a time.

## Contributing

- Build and test with the Maven Wrapper: `./mvnw verify`. Java 21 or later is required.
- The build has the modules `bth4j-core`, `bth4j-redis`, `bth4j-postgres`, `bth4j-sqlite`, `bth4j-micrometer`, `bth4j-admin-http` and `bth4j-testing`, released together under one version, plus `bth4j-store-tests` (the contract tests, not published). Unit tests run in the `test` phase. The integration tests (`*IT` in the store modules) start a Redis or PostgreSQL container with Testcontainers in the `integration-test` phase and need Docker.
- Versioning: the first change after a release bumps the version in `pom.xml` to the next `-SNAPSHOT`. A release is a pull request titled `Release X.Y.Z` that sets the final version and adds a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md`; merging it publishes the release.
