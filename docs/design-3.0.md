# bth4j 3.0 design

3.0 replaces the coordination protocol of 2.x. A task is always in exactly one state, every
transition between states is atomic together with the ownership check that guards it, and the whole
protocol is expressed against a store interface (the SPI), so the same services run on any store that
can make those transitions atomic. Nothing in 3.0 can read 2.x data: drain the 2.x queues before
upgrading, or use a new namespace (Redis) or table prefix (PostgreSQL).

Modules:

| Artifact | Content |
|---|---|
| `bth4j-core` | public API, runner, services, the store SPI (`today.bonfire.oss.bth4j.spi`); no dependency on a particular store |
| `bth4j-redis` | `RedisTaskStore`, Redis 7.0 or newer, Jedis |
| `bth4j-postgres` | `PostgresTaskStore`, PostgreSQL 9.5 or newer, `java.sql` only |
| `bth4j-testing` | `InMemoryStore` and `MutableClock` for tests of applications |
| `bth4j-store-tests` | `StoreContract`, the tests every store passes; not published |

Applications depend on `bth4j-core` and one store module.

## Goals

- At-least-once execution with a protocol that is easy to reason about: one state per task, one fence
  per ownership, no locks between instances.
- A small fixed cost per task. A task record has 14 always-present fields; everything else is written
  only when set, so a plain task costs about 100 bytes of fixed columns in PostgreSQL and 14 hash
  fields in Redis (a Redis hash whose payload exceeds 64 bytes leaves the compact encoding, where every
  field costs roughly 60 to 80 bytes of overhead, so field count matters more than value size).
- Identical behaviour on every store, enforced by one shared contract test (`StoreContract`).
- A producer API that survives lost replies: ids are known before the write, so an enqueue can be
  retried safely.
- Durable progress inside a task (`TaskContext.step`) so a retry resumes instead of starting over.
- No foreign keys, no time-of-check races in the application: integrity is the services' job, and every
  check that matters is inside the store's atomic call.

## Guarantees

- At least once. A task runs again only when the worker holding it crashed, stopped renewing its lease,
  or failed the attempt. A slow task never runs twice because it is slow, as long as its lease is
  renewed.
- A worker that lost its lease cannot complete, fail, reschedule or checkpoint the task; its calls are
  no-ops and report that the lease is lost. Only the holder of the current revision moves a LEASED task.
- An enqueue with an id that already exists changes nothing and is answered as accepted.
- One recurring definition produces each occurrence once, also with several instances.
- Shutdown waits for running tasks up to the shutdown timeout. Tasks still running after that keep their
  lease until it expires and are then retried.

## The task record

The same shape in both stores (`spi.TaskRecord`). Times marked "store" come from the store's own clock
(`now()` in PostgreSQL, `TIME` in a Redis script); "client" times come from the clock of the process
that built the request.

Always present:

| Field | Meaning |
|---|---|
| `id` | 64-bit, client-allocated in blocks (see Ids) |
| `revision` | the fence: +1 on every state change, never on renew, step or progress writes |
| `queueId` | queue registry id (smallint) |
| `eventId` | task type registry id (smallint); "event" is the stored name of a `TaskType` |
| `payloadVersion` | `TaskType.payloadVersion()` when the task was enqueued |
| `state` | code 0 to 6, see States |
| `attempts` | how often the task was claimed, minus attempts given back |
| `maxAttempts` | the task is dead after this many failed attempts |
| `created` | client time |
| `due` | store time; never null once stored |
| `stateChanged` | store time; when the current state was entered |
| `data` | encoded payload, at most 256 KiB; null when none or dropped on success |
| `workerId` | set while LEASED, kept on terminal rows, cleared on retry, reschedule and unclaim |
| `leaseUntil` | LEASED only |

Present only when set (a store never writes a null field):

| Field | Meaning |
|---|---|
| `claimedAt` | start of the last attempt; the run time of a finished task is `stateChanged - claimedAt` |
| `account` | application-defined owner label, at most 128 characters |
| `headers` | string map, at most 8 KiB |
| `uniqueKey`, `keyKeep`, `keyUntil` | unique key, its keep window, and when a held key of a finished task is released |
| `retryPolicy` | name of a retry policy; null means the runner's policy |
| `scheduleId` | the recurring definition this task is an occurrence of |
| `retryOf` | reserved for an admin retry link |
| `startBy`, `maxRun` | stored and read back; not enforced in this version |
| `cancelRequestedAt`, `cancelReason` | stored and read back; nothing sets them in this version |
| `progress` | free text set by the processor, at most 4096 characters |
| `steps` | completed steps by name (see Steps) |
| `failures` | the last 20 failed attempts, oldest first |

### due, stateChanged and revision

`due` has one meaning in every state: the time from which the task may run. For an immediate task it is
the enqueue time; for a scheduled task the requested time; after a failure the retry time. Promote and
claim keep it, and unclaim returns a task to its queue with its old `due`, so ordering inside a queue is
always `(due, id)` and a task put back is not overtaken by later ones. The earlier variants of this idea
(eligible, queued, fire time) are all `due`.

`stateChanged` is when the current state was entered. For terminal states it is the finish time, which is
what retention is measured from. It also orders the terminal and LEASED listings.

`revision` is the fence. Claim, promote, every terminal transition, reschedule and unclaim add one. Renew,
step writes and progress writes do not: the worker holds the revision its claim produced, and a long
running task that checkpoints does not invalidate its own fence. The revision therefore changes exactly
when ownership or state changes.

## States and transitions

| Code | State | Meaning |
|---|---|---|
| 0 | QUEUED | in its queue, runnable now |
| 1 | SCHEDULED | waiting for `due` (delayed enqueue, retry after failure, reschedule, recurring occurrence) |
| 2 | LEASED | claimed by a worker that holds the current revision |
| 3 | SUCCEEDED | finished; kept for the succeeded retention |
| 4 | DEAD | attempts exhausted or retry refused; kept for the dead retention |
| 5 | CANCELLED | ended by a cancel request |
| 6 | EXPIRED | start-by deadline passed before it ran |

States 3 to 6 are terminal. In this version the transitions into CANCELLED and EXPIRED do not exist:
the state codes, the retention for them (`cancelledRetention`), the `cancelRequested` flag on renew and
the purge are in place, but no store call produces them. A processor that throws
`TaskCancelledException` ends as DEAD (see Retry policy and failure history). They arrive with the admin
API in a later version without a schema change.

```
enqueue --(no due)--> QUEUED --claim--> LEASED --ack--> SUCCEEDED (or removed)
   |                     ^                |  |
   +--(due)--> SCHEDULED --promote--+     |  +--fail, attempts left--> SCHEDULED (due = retry time)
                  ^                       |  +--fail, exhausted-----> DEAD (or removed)
                  +-------reschedule------+  +--lease expired-------> same as fail
                  LEASED --unclaim--> QUEUED (due unchanged, attempt given back)
```

Each arrow is one store call, atomic with its checks:

- `enqueue` / `enqueueAll`: stores the task QUEUED, or SCHEDULED when `due` is in the future by store
  time. Answers `STORED`, `EXISTS` (the id exists in any state, nothing changed) or `DuplicateKey`.
- `claim`: takes up to n QUEUED tasks from the given queues, in the given queue order and by `(due, id)`
  inside a queue; a later queue is read only when the earlier ones could not fill n. It adds one to
  `attempts` and `revision`, sets `claimedAt`, `workerId` and `leaseUntil`, keeps `due`, and returns the
  records with payload and steps, so there is no second read.
- `renew`: extends the lease of every listed task whose revision still matches and that is LEASED, in one
  call, and reports per task whether it is still owned and whether a cancel was requested.
- `ack`: fence check, then SUCCEEDED, or the record is removed when the caller passes `keep = false`.
- `fail`: fence check, then SCHEDULED at the retry time, or DEAD when the retry time is null or the
  attempts are exhausted. The failure is appended to `failures` (last 20).
- `failExpired`: the same for a task found by `reclaim`; additionally a no-op when the lease is no longer
  expired, because the worker renewed it after the scan. Renewal does not change the revision, so the
  fence alone would let maintenance retry a task that is still running.
- `reschedule`: fence check, then SCHEDULED at the given time, attempt given back. Used by
  `TaskRescheduleException` and when a claimed task has no processor.
- `unclaim`: fence check, then QUEUED with the old `due`, attempt given back. Used when the executor
  rejects a claimed task.
- `promote`: moves SCHEDULED tasks whose `due` has passed to QUEUED, bounded, and returns the next due
  time so the scheduler sleeps exactly as long as needed.
- `saveSteps`, `progress`: fence check, then write one field; no revision change.
- `purge`, `releaseKeys`: see Retention.
- `putRecurring`, `deleteRecurring`, `dueRecurring`, `commitRecurring`: see Recurring definitions.

There are no locks between instances. Every transition is atomic and safe to retry, so instances race
and the loser's call reports `false`, `LOST` or `EXISTS`.

Every call a worker makes on a LEASED task (renew, ack, fail, reschedule, unclaim, saveSteps, progress)
carries the fence; the outcome of ack, fail and reschedule is reported to the observer as `APPLIED` or
`LOST`. If the store is unreachable when the outcome should be applied, the lease expires and maintenance
retries the task.

## Ids

Task ids are allocated by the client, not by the store at insert time. `TaskStore.allocateIds(count)`
reserves a block (Redis `INCRBY` on `seq:id`; PostgreSQL `nextval` on `<prefix>_task_id` under a row lock
so the block is contiguous). `TaskClient` hands them out from a block of `idBlockSize` (default 64), one
store call per block. Ids are unique, never reused and increasing within a process; across processes they
interleave, and an unused part of a block is lost when the process exits.

Knowing the id before the write makes enqueue retryable. If the store cannot be reached, the client does
not know whether the write happened. `TaskClient.enqueue` then throws `EnqueueFailedException`, which
carries the id; the caller enqueues the same request again with that id and the store answers `Accepted`
either way:

```java
long id = client.nextId();
var request = TaskRequest.of(SEND_MAIL, mail).id(id).build();
try {
    client.enqueue(request);
} catch (EnqueueFailedException e) {
    client.enqueue(request);          // same id: stored once whether or not the first write happened
}
```

`enqueueAll` reserves all ids before the first write and writes in chunks of 500; after a failure the
caller can retry the whole list, and tasks already stored answer accepted again. Note that a retried
request must carry its id: without `TaskRequest.id` each call allocates a new one and a lost reply would
store the task twice.

## Unique keys

A request with `uniqueKey` is stored only if no other task holds the key; otherwise nothing is stored and
the result is `Duplicate(existingId)`. The key lives on the task row (PostgreSQL: a partial unique index
`WHERE unique_key IS NOT NULL`; Redis: a string `k:<key>` holding the id), not in a table of its own.

A key is held while the task is not terminal. What happens afterwards is the `Uniqueness` of the request:

- `UNTIL_DONE` (default): the key is released at the transition to a terminal state.
- `keepFor(duration)`: the key stays held for the duration after the task became terminal (the keep
  window). `keyKeep` is stored on the task at enqueue; at the terminal transition `keyUntil =
  stateChanged + keyKeep` is set. Redis keeps `k:<key>` alive with `PEXPIRE`; PostgreSQL keeps the key
  column until `key_until`.

The window ends when `releaseKeys` clears it (maintenance runs it with every purge), or earlier when a new
enqueue finds an expired key and clears it inline. While a window is open the task row cannot be purged
(see Retention), and a task with a window is kept as SUCCEEDED or DEAD even if the retention is zero.

## Retention and purge

Terminal rows are kept so that they can be inspected, then removed by the purge. The runner configures a
retention per state:

| Setting | Default | States |
|---|---|---|
| `succeededRetention` | 48 hours | SUCCEEDED |
| `deadRetention` | 90 days | DEAD |
| `cancelledRetention` | 48 hours | CANCELLED and EXPIRED |

`MaintenanceStore.purge(state, retention, limit)` removes tasks of that state whose `stateChanged` is older
than `now - retention`, oldest first, using store time. The retention is not stored on the row: it is the
current setting of whichever runner purges, so changing it applies to existing rows. A task whose
`keyUntil` is still in the future is skipped, and the skipped rows do not starve the purge (Redis pages
behind them; PostgreSQL filters them in the query).

A retention of ZERO removes the record at the transition itself: the runner passes `keep = false` to `ack`
and `keepDead = false` to `fail`, and the row is deleted in the same atomic call. The exception is a task
holding a keep window, which is kept (its payload dropped for SUCCEEDED) until the window ends and the
purge removes it. A succeeded task also drops its payload (`data`) unless `keepSucceededPayload` is set;
its `steps` are kept.

Redis task hashes never get a TTL; the purge is the only remover. The purge runs every `purgeInterval`
(default 1 minute) in batches of `purgeBatchSize` (default 500) until a batch removes fewer.

## Steps

`TaskContext.step(name, Class<R>, Callable<R>)` and `step(name, Step)` split a processor into checkpoints.
When the task's `steps` already contain `name`, the stored result is returned and the body does not run.
Otherwise the body runs, its result is encoded with the runner's JSON mapper, and the complete steps map
is written with `saveSteps(id, revision, steps)` before `step` returns. A later attempt resumes after the
last completed step, because `claim` returns the steps with the record.

The guarantee is at least once per step: if the worker dies after the body finished and before the write,
the step runs again. The rule for processors is therefore: put side effects inside steps, and make each
step idempotent. Work outside a step runs on every attempt.

Limits: step name 1 to 64 characters, result 8192 characters, all steps together 64 KiB (counted as name
plus result characters), checked in the client before the write. `saveSteps` carries the fence and does
not change the revision; if it returns `false`, `step` throws `TaskLeaseLostException`, because the task
now belongs to another worker. Before running a new step the runner also checks the cancel and lease-lost
flags. A step result of null is stored as a step without result. Steps are kept on SUCCEEDED rows and on
DEAD rows for inspection.

## Retry policy and failure history

When a processor throws, the runner maps the exception:

| Thrown | Effect |
|---|---|
| returns normally | `ack` |
| `TaskRescheduleException(delay)` | `reschedule` at now + delay, attempt given back |
| `TaskUnrecoverableException` | `fail` with no retry time: DEAD |
| `TaskDataException` (payload cannot be read) | DEAD |
| `TaskLeaseLostException` | nothing is written; the task belongs to someone else |
| `TaskCancelledException` | `fail` with no retry time: DEAD (no CANCELLED transition exists yet) |
| any other exception | the retry policy decides |

`RetryPolicy.decide(task, error)` returns `RetryAfter(delay)` or `Dead(reason)`. The default is
exponential backoff with jitter, `RetryPolicy.exponential(1 s, 1 h)`: 1 s doubling per failed attempt,
capped at one hour, varied by up to 20 percent. `fixed(delay)` is also provided. The runner ends the task
as DEAD anyway once `attempts >= maxAttempts`, whatever the policy returned. The retry time is the runner
clock plus the delay and is written as `due` of the SCHEDULED task.

Every failed attempt appends a `FailureInfo(attempt, at, type, message, stack)` to `failures`, which the
store keeps to the last 20 (oldest dropped). The message is bounded to 2 KiB and the stack trace to
16 KiB. A lease that expired is recorded with type `lease-expired`. The failures stay on a DEAD row.

When maintenance reclaims an expired lease it asks the policy with a null error; a `RetryAfter` becomes
the retry time, anything else (or no attempts left) makes the task DEAD.

## Unknown events

A runner claims only the events it has processors for (`ClaimRequest.knownEventIds`). A task of another
event can sit in a queue the runner reads, for example during a rolling deploy where only some instances
know a new task type. Each store handles it in its cheapest way, and all three leave the task QUEUED and
spend no attempt:

- PostgreSQL filters in the claim query (`event_id = ANY(...)`); the rows are never touched.
- Redis cannot filter a zset by a hash field, so the claim script skips the task and moves its queue
  score 5 seconds ahead, which keeps it from being re-read on every poll. The `due` field is unchanged.
- The in-memory store skips it.

If a claimed task still has no processor (a registry race), the runner reschedules it 5 seconds ahead
with the attempt given back and emits `Deferred`.

## Recurring definitions

`TaskClient.putSchedule(ScheduleDefinition)` stores a definition: id, task type, payload, 5-field Unix
cron expression evaluated in UTC, queue, account and max attempts. A definition is not a task; each
occurrence becomes a task of its own with `scheduleId` set.

The recurring service reads definitions whose next fire time is within one check interval
(`recurringTasksCheckInterval`, default 10 s) from now, builds an occurrence (a SCHEDULED task with
`due = fireAt` and a new id) and calls `commitRecurring(due, nextFire, occurrence)`. The store stores the
occurrence and advances the definition in one atomic step, but only if the definition's `revision` and its
stored fire time still equal what the caller read. The two compare-and-set values make the commit safe
under any number of racing instances: the first wins, the others get `false`.

Missed occurrences are skipped. The next fire time is computed from `max(fireAt, now)`, so if every runner
was down for a day, the definition fires once for the stored fire time (a task whose `due` is in the past,
which runs at once) and then continues from now; it does not replay the days in between. A definition whose
expression never fires again is deleted.

`putRecurring` assigns a new revision on every put, taken from a counter that never resets (Redis
`cronrev`, PostgreSQL sequence `<prefix>_schedule_rev`), also after a delete, so a runner holding a stale
definition can never commit against its replacement. Registering again with the same cron expression
keeps the stored next fire time; a changed expression resets it.

## Registries and the no-foreign-keys rule

Queue names, task type names and workers are stored as numbers on the task row and resolved through
registries:

| Registry | Id type | Redis | PostgreSQL |
|---|---|---|---|
| queue | smallint, 1 to 32767 | `reg:queue` (name to id), `reg:queue:names` (id to name), `seq:queue`, `queues` set | `<prefix>_queue(id smallserial, name unique, paused)` |
| event | smallint, 1 to 32767 | `reg:event`, `reg:event:names`, `seq:event` | `<prefix>_event(id smallserial, name unique)` |
| worker | integer | `seq:worker`, `w:<id>` hash (label, started) | `<prefix>_worker(id serial, label, started_at, last_seen)` |

Ids are permanent and never reused, so `Names` caches both directions for the life of the process; a miss
in the reverse direction reloads the registry once. Resolution reads first and registers only a new name,
because on PostgreSQL an insert that loses a conflict still consumes a sequence value and would exhaust a
smallint sequence. Both stores refuse beyond 32767 names. The runner registers all its queues and the
events of its processors at start; the client registers lazily on the first use of a name. Every runner
start allocates a new worker id with its label (`workerLabel`, default the JVM name `pid@host`).

There are no foreign keys in any table, and none in Redis by nature. The registries are written only
through `queueId` and `eventId`; the services are the only writers of the task rows. Dashboards and ad hoc
SQL join the registry tables by id. The `paused` column of the queue table and `last_seen` of the worker
table are reserved for later versions and unused now.

## Redis layout

All keys carry the namespace as a hash tag, `{ns}:`, so everything lives in one Cluster slot. Every
script receives the prefix as its first argument and derives all other keys from it; the single declared
key only routes the call.

| Key | Type | Content |
|---|---|---|
| `meta` | hash | `schema` (version) |
| `seq:id`, `seq:worker`, `seq:queue`, `seq:event` | string | counters |
| `reg:queue`, `reg:queue:names`, `reg:event`, `reg:event:names` | hash | registries |
| `queues` | set | queue ids that exist (the purge walks these) |
| `w:<id>` | hash | worker: label, started |
| `t:<id>` | hash | the task; only non-null fields; no TTL |
| `q:<qid>` | zset | QUEUED ids, score `due` ms |
| `scheduled` | zset | SCHEDULED ids, score `due` ms |
| `leased` | zset | LEASED ids, score lease expiry ms |
| `idx:<state>:<qid>` | zset | ids in LEASED and the terminal states, score `stateChanged` ms |
| `c:<qid>` | hash | counters, one field per state code |
| `k:<key>` | string | id of the task that holds a unique key; `PEXPIRE` while a keep window runs |
| `keys` | zset | finished tasks whose hash still names a key, score `keyUntil` ms |
| `cron`, `cron:<sid>`, `cronrev` | zset, hash, string | next fire times, definitions, revision counter |

Task hash fields: `rev`, `q`, `ev`, `pv`, `state`, `att`, `max`, `created`, `due`, `sc`, `data`, `wid`,
`lease` always or when applicable; `ca` (claimedAt), `acc`, `hdr`, `uk`, `kk`, `ku`, `rp`, `sid`, `rof`,
`sb`, `mr`, `cra`, `crs`, `prg`, `steps`, `fl` only when set. Times are epoch milliseconds.

One script per transition. `RedisScripts` has one script for each store call (enqueue, claim, renew, ack,
fail, reschedule, unclaim, set field, promote, reclaim, purge, release keys, register, and the four
recurring operations). Each script reads the clock with `redis.call('TIME')` inside the script, so due
times, leases and retention all use Redis time and not the client's. Reading the clock before a write
requires effect replication, the default since Redis 7.0, which is the reason for the version
requirement. Each state change goes through the same three helpers (`move`, `enter`, `leave`), which keep
the hash, the structure that represents the state and the `c:` counter together.

A script is called by its SHA-1 with `EVALSHA`; on `NOSCRIPT` it is loaded once with `SCRIPT LOAD`,
routed by the declared key. Queue order is by score `due`; equal scores tie by member bytes (the id as
text), which is accepted. A claim scans a queue for at most `count * 4` ids (cap 1000), so a block of
unknown-event tasks cannot make one claim unbounded.

## PostgreSQL schema

Tables per prefix (lower case letters, digits and underscores, at most 41 characters), created by `init()`
or by running `schemaStatements()` from a migration tool:

| Table | Content |
|---|---|
| `<prefix>_meta` | `schema_version` |
| `<prefix>_queue`, `<prefix>_event`, `<prefix>_worker` | registries (above) |
| `<prefix>_task` | one row per task |
| `<prefix>_schedule` | recurring definitions: `id`, `event_id`, `payload_version`, `queue_id`, `account`, `cron`, `data`, `max_attempts`, `rev`, `next_fire` |

Task columns: `id bigint PK`, `revision integer`, `queue_id smallint`, `event_id smallint`,
`payload_version smallint`, `state smallint`, `attempts smallint`, `max_attempts smallint`, `created`,
`due`, `state_changed` (all `timestamptz`), `data text`, `worker_id integer`, `lease_until`, `claimed_at`,
`account text`, `headers jsonb`, `unique_key text`, `key_keep_ms bigint`, `key_until`, `retry_policy text`,
`schedule_id text`, `retry_of bigint`, `start_by`, `max_run_ms integer`, `cancel_requested_at`,
`cancel_reason text`, `progress text`, `steps jsonb`, `failures jsonb`. The columns `id` through
`state_changed` are `NOT NULL`; `data`, `worker_id`, `lease_until` and everything after them are nullable
and null unless set.

Sequences: `<prefix>_task_id` (ids), `<prefix>_schedule_rev` (definition revisions), plus the sequences of
the `smallserial` and `serial` registry columns.

Partial indexes, one per service scan:

| Index | Serves |
|---|---|
| `(queue_id, due, id) WHERE state = 0` | claim |
| `(due) WHERE state = 1` | promote and next due |
| `(lease_until) WHERE state = 2` | reclaim |
| `(state, state_changed, id) WHERE state >= 3` | purge and terminal listings |
| `(unique_key) UNIQUE WHERE unique_key IS NOT NULL` | unique keys |
| `(key_until) WHERE key_until IS NOT NULL` | release of expired keys |
| `(start_by) WHERE start_by IS NOT NULL AND state < 2` | the later expiry transition |
| `<prefix>_schedule (next_fire)` | due definitions |

Each store call is one statement or one short transaction, so its check and its writes commit together;
fenced transitions are `UPDATE ... WHERE id = ? AND revision = ? AND state = 2` and the update count is the
answer. `fail` locks the row first, because it appends to the failures it reads. `enqueueAll` is one
`INSERT ... SELECT FROM unnest(...) WITH ORDINALITY ... ON CONFLICT DO NOTHING RETURNING id`; the conflict
target is left out so that it covers both the id and the unique key, and the code tells the two apart
afterwards.

Claim query shape. A bounded, index-ordered scan per queue, then the caller's queue order decides between
them:

```
WITH q      AS (SELECT queue_id, prio FROM unnest(:queue_ids) WITH ORDINALITY AS q(queue_id, prio)),
     locked AS (SELECT c.id, q.prio, c.due FROM q, LATERAL (
                  SELECT id, due FROM task
                  WHERE state = 0 AND queue_id = q.queue_id [AND event_id = ANY(:events)]
                  ORDER BY due, id LIMIT :count FOR UPDATE SKIP LOCKED) c),
     picked AS (SELECT id, row_number() OVER (ORDER BY prio, due, id) AS ord
                FROM (SELECT * FROM locked ORDER BY prio, due, id LIMIT :count) l),
     upd    AS (UPDATE task t SET state = 2, revision = revision + 1, attempts = attempts + 1,
                  worker_id = :worker, claimed_at = now(), state_changed = now(),
                  lease_until = now() + :lease
                FROM picked WHERE t.id = picked.id RETURNING t.*, picked.ord)
SELECT * FROM upd ORDER BY ord
```

A single `ORDER BY array_position(...)` over all queues would sort the whole backlog on every claim. The
`LATERAL` form walks the `(queue_id, due, id)` index to at most `count` rows per queue. Concurrent runners
skip each other's locked rows. The lock is taken on up to `count` rows per queue, of which at most `count`
are claimed; the others are released at commit.

Features used and the version they set: `FOR UPDATE SKIP LOCKED` and `ON CONFLICT` (9.5), `jsonb`,
`WITH ORDINALITY` and multi-array `unnest` (9.4), `LATERAL` (9.3), `smallserial` (9.2), data-modifying
CTEs and advisory locks (9.1). The minimum is therefore 9.5; the tests run on 17.

Times are `timestamptz` and are written with millisecond precision so a value read back equals the value
written. `init()` takes a transaction-level advisory lock per prefix, so several runners starting at once
create the schema once, then checks `schema_version` and refuses another version or a task table without
a meta table.

## Concurrency model

`BackgroundRunner.Builder.concurrency` takes a `Concurrency`:

- `Concurrency.fixed(n)`: at most n tasks at once, 1 to 100000. This is the default, with
  `n = max(2, 2 * availableProcessors)`. Every task runs on its own virtual thread, so the limit protects
  the resources a task uses, not the thread count.
- `Concurrency.adaptive(min, max)`: a limit that moves between min and max with CPU load and throughput,
  evaluated once per second (`DefaultVtExecutor`): it shrinks above 80 percent CPU and grows while
  throughput improves.

A custom `BackgroundExecutor` can replace both. Its contract:

```java
int reserve(int max, Duration wait) throws InterruptedException; // 1..max, or 0 on timeout
void release(int slots);
void execute(Runnable task);   // consumes one reserved slot
int activeCount();
void shutdown();
boolean awaitTermination(Duration timeout) throws InterruptedException;
```

The processor thread reserves executor slots first, then claims at most that many tasks in one call, so a
claimed task always has a slot. One claim covers every queue the runner processes, so a poll is one store
call whatever the queue count. When the claim returns nothing the thread sleeps for `idlePollInterval`
(default 500 ms, minimum 10 ms); a busy runner never sleeps; a full executor makes it wait for a slot, not
for a timer. Queues are processed in the order given to `configureQueues`, rotated by one position on
every claim, so a queue listed twice gets two turns.

There is deliberately no push signal from the store. A wake-up channel costs every enqueue a fan-out to
every runner (and on PostgreSQL serializes the commits of notifying transactions), which is paid exactly
in the busy systems that never idle, while an idle claim is a sub-millisecond call that a shorter poll
interval makes as frequent as needed.

Leases: a claimed task is leased for `leaseDuration` (default 60 s, minimum 1 s). A renewal thread extends
the leases of all running tasks every third of that duration in one call, and passes cancel requests on to
running executions. Maintenance (`maintenanceCheckInterval`, default 5 s) finds expired leases with
`reclaim` and fails them with `failExpired`. Clock skew between instances only matters for the client-side
instants (`created`, request `at`/`delay`, retry times); leases, due times and retention use the store
clock.

## Lifecycle

- `build()` creates the client and calls `store.init()`; nothing else touches the store.
- `start()` fails with `TaskConfigurationError` when no processor is registered. It freezes the registry,
  allocates the worker id, resolves queue and event ids, starts the four services (processor, scheduled,
  recurring, maintenance) and a monitor that restarts a service whose thread died every
  `serviceCheckInterval` (default 10 s). It returns after starting. A second call, or a call after
  `stop()`, logs a warning and does nothing.
- `quiet()` stops claiming; running tasks finish and the other services keep going (due tasks are still
  promoted, leases still renewed and reclaimed, occurrences still created). `resume()` claims again.
- `stop()` stops claiming and all services, waits for each up to `shutdownTimeout` (default 30 s), then shuts
  the executor down and waits for running tasks, still renewing their leases. Tasks still running afterwards
  keep their lease until it expires and are then retried. A second call is a no-op.

Several runners on one store share the work; `TaskClient.builder()` gives a process that only produces a
client without a runner.

## The store SPI and how to implement a store

The SPI is `today.bonfire.oss.bth4j.spi`, split by who calls it; a store module implements all of them
through `FullStore`:

| Interface | Content |
|---|---|
| `TaskStore` | `info`, `init`, `allocateIds`, `allocateWorkerId`, `queueId`, `eventId`, `queueNames`, `eventNames` |
| `ExecutionStore` | `enqueue`, `enqueueAll`, `claim`, `renew`, `ack`, `fail`, `failExpired`, `reschedule`, `unclaim`, `saveSteps`, `progress` |
| `MaintenanceStore` | `promote`, `reclaim`, `purge`, `releaseKeys`, `nextDue` |
| `ScheduleStore` | `putRecurring`, `deleteRecurring`, `recurringSchedules`, `dueRecurring`, `commitRecurring` |
| `InspectionStore` | `getTask`, `counts`, `tasks` (by state, keyset paging on `(stateChanged, id)`) |

The contract is small because the services never read and then write in two steps: each method is one
atomic transition with its own check. What a store needs from its engine is a conditional multi-row write
(compare the revision, then move the task), a way to take n queued rows exclusively, and its own clock. On
Redis that is one Lua script per method; on an SQL database one statement per method, with
`FOR UPDATE SKIP LOCKED` for the claim and `UPDATE ... WHERE id = ? AND revision = ? AND state = 2` for the
fenced transitions.

Rules every store follows:

- A store never writes a null field. Steps and failures are encoded to JSON by the core (`RecordJson`);
  the store only fences and writes the whole field, so step logic is not duplicated per engine.
- Time for `due` defaults, leases, promote, reclaim and retention is the store's clock.
- Calls are made concurrently from many threads and processes; each must be atomic and safe to repeat.
- A failure to reach the engine throws `TaskStoreException`; callers retry on their next cycle.
- `init()` creates the schema or verifies `StoreInfo.SCHEMA_VERSION` (currently 1) and refuses another one.

The tests are `StoreContract` in `bth4j-store-tests`. A store module subclasses it, supplies a fresh,
isolated store per test from `newStore(namespace)` and, when the test can move time, the clock from
`clock()`. The in-memory store runs it in `bth4j-testing`, the Redis and PostgreSQL stores run it as
integration tests against real servers (Testcontainers).

## What is deliberately not in this version

- An admin API and a dashboard. The store and the records already carry what they need (`counts`, `tasks`,
  failures, steps, progress, the worker registry), but there are no operator actions.
- Cancellation and expiry transitions. CANCELLED and EXPIRED exist as states with retention; no call
  produces them, and `startBy`, `maxRun`, `cancelRequestedAt` and `cancelReason` are stored but not acted on.
- Per-queue concurrency limits and queue pausing (the `paused` column is unused).
- Time zones in cron expressions: UTC only.
- Task chains and workflows; use steps inside one task, or enqueue the next task from a processor.
- Worker heartbeats (`last_seen` is written once at registration).
- Named retry policies: `TaskRequest.retryPolicy(String)` is stored, but the runner uses its one policy.
- A push signal from the store to idle runners (see Concurrency model).
