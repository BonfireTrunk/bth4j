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
| `bth4j-micrometer` | `MicrometerTaskObserver` and `MicrometerRunnerMetrics`; `micrometer-core` is provided |
| `bth4j-admin-http` | `AdminHttpServer`, a JSON API over `AdminOps` on the JDK HTTP server |
| `bth4j-store-tests` | `StoreContract` and `RunnerContract`, the tests every store passes; not published |

The `dashboard/` directory is a static Vue app for the admin HTTP API; it is not a Maven module.

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
| `startBy`, `maxRun` | expiry and run-time limit, see Expiry and maximum run time |
| `cancelRequestedAt`, `cancelReason` | set by a cancel request (or by `finishCancelled`); see Cancel protocol |
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

States 3 to 6 are terminal. CANCELLED is entered by `AdminStore.cancel` (from QUEUED or SCHEDULED) and by
`ExecutionStore.finishCancelled` (from LEASED); EXPIRED by `MaintenanceStore.expire` and, inline, by a claim
that meets a task past its `startBy`. `AdminStore.retry` leaves DEAD, CANCELLED and EXPIRED for QUEUED. See
Cancel protocol and Expiry and maximum run time.

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
- `claim`: skips paused queues and never returns a task whose `startBy` has passed. It takes up to n QUEUED tasks from the given queues, in the given queue order and by `(due, id)`
  inside a queue; a later queue is read only when the earlier ones could not fill n. It adds one to
  `attempts` and `revision`, sets `claimedAt`, `workerId` and `leaseUntil`, keeps `due`, and returns the
  records with payload and steps, so there is no second read.
- `renew`: extends the lease of every listed task whose revision still matches and that is LEASED, in one
  call, and reports per task whether it is still owned and whether a cancel was requested.
- `finishCancelled`: fence check, then CANCELLED; see Cancel protocol.
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
- `expire`: moves QUEUED and SCHEDULED tasks whose `startBy` passed to EXPIRED; see Expiry and maximum run time.
- `promote`: moves SCHEDULED tasks whose `due` has passed to QUEUED, bounded, and returns the next due
  time so the scheduler sleeps exactly as long as needed.
- `saveSteps`, `progress`: fence check, then write one field; no revision change.
- `purge`, `releaseKeys`: see Retention.
- `putRecurring`, `deleteRecurring`, `dueRecurring`, `commitRecurring`, `pauseRecurring`, `setNextFire`,
  `recurringDefinitions`: see Recurring definitions and Schedules.
- `AdminStore` calls (`cancel`, `retry`, `delete`, `pauseQueue`, `heartbeat`, `queues`, `workers`): see The
  admin SPI.

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
| `succeededRetention` | 10 minutes | SUCCEEDED |
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
| `TaskCancelledException` | `finishCancelled`: CANCELLED, see Cancel protocol |
| any exception while a cancel was requested | `finishCancelled` as well: the processor most likely stopped because of the request |
| any other exception | the retry policy decides |

`RetryPolicy.decide(task, error)` returns `RetryAfter(delay)` or `Dead(reason)`. The runner holds the default
policy and the named ones given to `BackgroundRunner.builder().retryPolicy(name, policy)`; the execution picks
`record.retryPolicy()` from that map (`service.RetryPolicies`), and a null name means the default. An unknown
name uses the default policy and records the issue `retry-policy`; so does a policy that throws, which retries
at once. The default is
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

`TaskClient.putSchedule(ScheduleDefinition)` stores a definition: id, task type, payload, a 5-field Unix
cron expression evaluated in a zone (UTC by default) or a fixed delay, misfire policy, overlap mode, queue,
account and max attempts. A definition is not a task; each
occurrence becomes a task of its own with `scheduleId` set.

The recurring service reads definitions whose next fire time is within one check interval
(`recurringTasksCheckInterval`, default 10 s) from now, builds an occurrence (a SCHEDULED task with
`due = fireAt` and a new id) and calls `commitRecurring(due, nextFire, occurrence)`. The store stores the
occurrence and advances the definition in one atomic step, but only if the definition's `revision` and its
stored fire time still equal what the caller read. The two compare-and-set values make the commit safe
under any number of racing instances: the first wins, the others get `false`.

A fire time that is late by no more than the misfire threshold is an ordinary fire: the next fire time is
computed from `max(fireAt, now)`. A later one is a misfire, handled by the definition's policy (see
Schedules). A definition whose expression never fires again keeps its last occurrence and its next fire time
is parked 100 years ahead.

`putRecurring` assigns a new revision on every put, taken from a counter that never resets (Redis
`cronrev`, PostgreSQL sequence `<prefix>_schedule_rev`), also after a delete, so a runner holding a stale
definition can never commit against its replacement. Registering again with the same cron expression
keeps the stored next fire time as long as the timing (expression, zone, fixed delay) is unchanged; changed
timing resets it. A re-put keeps the paused flag the definition has; a new definition takes the flag as given.

## Registries and the no-foreign-keys rule

Queue names, task type names and workers are stored as numbers on the task row and resolved through
registries:

| Registry | Id type | Redis | PostgreSQL |
|---|---|---|---|
| queue | smallint, 1 to 32767 | `reg:queue` (name to id; listings invert it, purge walks its values), `seq:queue` | `<prefix>_queue(id smallserial, name unique, paused)` |
| event | smallint, 1 to 32767 | `reg:event` (name to id), `seq:event` | `<prefix>_event(id smallserial, name unique)` |
| worker | integer | `seq:worker`, `w:<id>` hash (label, started, seen, running), `workers` zset | `<prefix>_worker(id serial, label, started_at, last_seen, running)` |

Ids are permanent and never reused, so `Names` caches both directions for the life of the process; a miss
in the reverse direction reloads the registry once. Resolution reads first and registers only a new name,
because on PostgreSQL an insert that loses a conflict still consumes a sequence value and would exhaust a
smallint sequence. Both stores refuse beyond 32767 names. The runner registers all its queues and the
events of its processors at start; the client registers lazily on the first use of a name. Every runner
start allocates a new worker id with its label (`workerLabel`, default the JVM name `pid@host`).

There are no foreign keys in any table, and none in Redis by nature. The registries are written only
through `queueId` and `eventId`; the services are the only writers of the task rows. Dashboards and ad hoc
SQL join the registry tables by id. The `paused` column of the queue table is written by `AdminStore.pauseQueue`; `last_seen` and `running`
of the worker table by `heartbeat` (see Workers).

## Redis layout

All keys carry the namespace as a hash tag, `{ns}:`, so everything lives in one Cluster slot. Every
script receives the prefix as its first argument and derives all other keys from it; the single declared
key only routes the call.

| Key | Type | Content |
|---|---|---|
| `meta` | hash | `schema` (version) |
| `seq:id`, `seq:worker`, `seq:queue`, `seq:event` | string | counters |
| `reg:queue`, `reg:event` | hash | registries, name to id |
| `w:<id>` | hash | worker: `label`, `started`, `seen`, `running` |
| `workers` | zset | worker ids, score last heartbeat ms |
| `paused` | set | queue ids that claims skip |
| `startby` | zset | QUEUED and SCHEDULED ids that have a `sb`, score `sb` ms |
| `t:<id>` | hash | the task; only non-null fields; no TTL |
| `q:<qid>` | zset | QUEUED ids, score `due` ms |
| `scheduled` | zset | SCHEDULED ids, score `due` ms |
| `leased` | zset | LEASED ids, score lease expiry ms |
| `idx:<state>:<qid>` | zset | ids in LEASED and the terminal states, score `stateChanged` ms |
| `c:<qid>` | hash | counters, one field per state code |
| `k:<key>` | string | id of the task that holds a unique key; `PEXPIRE` while a keep window runs |
| `keys` | zset | finished tasks whose hash still names a key, score `keyUntil` ms |
| `cron`, `cron:<sid>`, `crondefs`, `cronrev` | zset, hash, set, string | next fire times of definitions that are not paused, definitions, ids of all definitions, revision counter |

Task hash fields: `rev`, `q`, `ev`, `pv`, `state`, `att`, `max`, `created`, `due`, `sc`, `data`, `wid`,
`lease` always or when applicable; `ca` (claimedAt), `acc`, `hdr`, `uk`, `kk`, `ku`, `rp`, `sid`, `rof`,
`sb`, `mr`, `cra`, `crs`, `prg`, `steps`, `fl` only when set. Times are epoch milliseconds.

Definition hash `cron:<sid>`: `ev`, `q`, `pv`, `max`, `rev`, `next` (the next fire, kept also while
paused) and the optional `acc`, `data`, `cron`, `zone`, `mf` (misfire), `cum` (catch-up max), `fd` (fixed delay ms), `os` (overlap skip, `1`) and
`paused` (`1`).

One script per transition. `RedisScripts` has one script for each call that changes state under the fence
(ENQUEUE, CLAIM, RENEW, ACK, FAIL, RESCHEDULE, UNCLAIM, SET_FIELD, PROMOTE, PURGE, RELEASE_KEYS, REGISTER,
the recurring operations CRON_PUT, CRON_DELETE, CRON_COMMIT, CRON_PAUSE and CRON_SET_NEXT, and the wave 2
scripts EXPIRE, FINISH_CANCELLED, CANCEL, RETRY, DELETE, WORKER_REGISTER and PURGE_WORKERS; 24 in all). The
reads and the heartbeat are plain commands: `reclaim` is a ZRANGE on `leased` followed by one pipelined HMGET
per id (an entry whose hash is gone is removed from `leased` on the spot), `dueRecurring` is a ZRANGE on `cron`
followed by pipelined HGETALLs (a definition deleted between the two reads is skipped), and `heartbeat` is a
ZADD XX CH on `workers` followed by a HSET when the worker still exists. The two that compare against scores
written by scripts (`reclaim`, `heartbeat`) first read the server clock with TIME, so every comparison uses
Redis time. Each script reads the clock with `redis.call('TIME')` inside the script, so due
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
| `<prefix>_queue`, `<prefix>_event`, `<prefix>_worker` | registries (above); `_queue.paused` and `_worker.running` (`integer NOT NULL DEFAULT 0`) belong to the admin SPI |
| `<prefix>_task` | one row per task |
| `<prefix>_schedule` | recurring definitions: `id`, `event_id`, `payload_version`, `queue_id`, `account`, `cron`, `zone`, `misfire`, `catch_up_max`, `fixed_delay_ms`, `overlap_skip`, `data`, `max_attempts`, `paused`, `rev`, `next_fire` |

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
| `(start_by) WHERE start_by IS NOT NULL AND state < 2` | `expire` |
| `<prefix>_schedule (next_fire) WHERE NOT paused` (`_due`) | due definitions |

Each store call is one statement or one short transaction, so its check and its writes commit together;
fenced transitions are `UPDATE ... WHERE id = ? AND revision = ? AND state = 2` and the update count is the
answer. `PostgresTaskStore.enqueue(Connection, TaskRecord)` runs the same insert on the caller's connection
and neither commits nor closes it, so a task prepared with `TaskClient.prepare` can join the application's
transaction. `fail` locks the row first, because it appends to the failures it reads. `enqueueAll` is one
`INSERT ... SELECT FROM unnest(...) WITH ORDINALITY ... ON CONFLICT DO NOTHING RETURNING id`; the conflict
target is left out so that it covers both the id and the unique key, and the code tells the two apart
afterwards.

Claim query shape (the real query also leaves out the queue ids in `<prefix>_queue` with `paused`, and rows
with `start_by <= now()`). A bounded, index-ordered scan per queue, then the caller's queue order decides
between them:

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
every claim, so a queue listed twice gets two turns. A runner with a `QueuePolicy` claims differently, see
Queue policies and pausing.

There is deliberately no push signal from the store. A wake-up channel costs every enqueue a fan-out to
every runner (and on PostgreSQL serializes the commits of notifying transactions), which is paid exactly
in the busy systems that never idle, while an idle claim is a sub-millisecond call that a shorter poll
interval makes as frequent as needed.

Leases: a claimed task is leased for `leaseDuration` (default 60 s, minimum 1 s). A renewal thread extends
the leases of all running tasks every third of that duration in one call, passes cancel requests on to
running executions and requests the cancellation of tasks past their `maxRun`. Maintenance (`maintenanceCheckInterval`, default 5 s) finds expired leases with
`reclaim` and fails them with `failExpired` (or ends them with `finishCancelled` when a cancel was
requested), expires tasks past their `startBy` and writes the worker heartbeat. Clock skew between instances only matters for the client-side
instants (`created`, request `at`/`delay`, retry times); leases, due times and retention use the store
clock.

## Lifecycle

- `build()` creates the client and calls `store.init()`; nothing else touches the store.
- `start()` fails with `TaskConfigurationError` when no processor is registered. It freezes the registry,
  allocates the worker id, resolves queue and event ids (also those of the queue policies), starts the four services (processor, scheduled,
  recurring, maintenance) and a monitor that restarts a service whose thread died every
  `serviceCheckInterval` (default 10 s). It returns after starting. A second call, or a call after
  `stop()`, logs a warning and does nothing.
- `quiet()` stops claiming; running tasks finish and the other services keep going (due tasks are still
  promoted, leases still renewed and reclaimed, occurrences still created). `resume()` claims again.
- `drain(timeout)` is `quiet()` followed by a wait until no task runs on this instance or the timeout passes;
  it returns whether the runner drained and leaves it quiet.
- `stop()` stops claiming and all services, waits for each up to `shutdownTimeout` (default 30 s), then shuts
  the executor down and waits for running tasks, still renewing their leases. Tasks still running afterwards
  keep their lease until it expires and are then retried. A second call is a no-op.

Several runners on one store share the work; `TaskClient.builder()` gives a process that only produces a
client without a runner.

## Cancel protocol

A cancel has two shapes, decided by the state the task is in when `AdminStore.cancel(id, revision, reason)`
runs. The call checks the revision first; a mismatch answers `STALE` and changes nothing.

- QUEUED or SCHEDULED: no worker holds the task, so it becomes CANCELLED at once (revision +1,
  `stateChanged`, `cancelRequestedAt = now`, `cancelReason`, unique key handled like any terminal transition,
  the start-by index entry removed). Answer `CANCELLED`.
- LEASED: only the request is stored (`cancelRequestedAt = now`, `cancelReason`); the revision does not
  change, because the worker's fence must stay valid. Answer `REQUESTED`.
- Any other state answers `STALE`.

The worker learns of a request through `renew`, which reports per task whether it is owned and whether
`cancelRequestedAt` is set (Redis: the script returns 2 instead of 1). The renewal thread calls
`requestCancel` on the execution, which sets the `cancelRequested` flag read by `TaskContext.cancelRequested()`
and `checkCancelled()` (also checked before every new step), and interrupts the processor's thread when its
`TaskType` was declared with `interruptOnCancel()`. The interrupt flag is cleared when the processor returns, so
it cannot leak into the executor thread's next task. Latency from the operator's call to the processor
noticing is up to one renewal period, a third of `leaseDuration`.

The task ends in `ExecutionStore.finishCancelled(id, revision, reason, keepPayload)`: LEASED with a matching
revision becomes CANCELLED (revision +1, `stateChanged`, `cancelReason` set when given, `cancelRequestedAt`
kept or set to now, `workerId` kept, `leaseUntil` cleared, payload dropped unless kept, unique key handled
like `ack`). A mismatch returns false and is reported to the observer as `Outcome.LOST`, like every fenced
transition. The runner calls it when the processor throws `TaskCancelledException`, and also when it throws
any exception while the flag is set, because it most likely stopped because of the request (interrupt,
closed resource). A processor that returns normally despite the request is acked as SUCCEEDED. The reason is
the text the runner holds (the maximum run time message, or the processor's exception); when it holds none,
because the request arrived through `renew`, which carries no text, the operator's reason is read from the
record. The default
reason of `AdminOps.cancel` is "cancelled by operator" (at most 512 characters). The observer receives
`TaskEvent.Cancelled(task, reason, outcome)`.

If the worker dies, nobody calls `finishCancelled`. Maintenance finds the expired lease with `reclaim`, which
reports `cancelRequested`, and ends the task with `finishCancelled` instead of `failExpired`, so a cancelled
task is never retried.

`retry` is the way back: DEAD, CANCELLED or EXPIRED with a matching revision becomes QUEUED, `due = now`,
`attempts = 0`, revision +1; the cancel fields, `workerId`, `leaseUntil`, `startBy`, `keyKeep`, `keyUntil` and
`uniqueKey` are cleared; failures and steps stay. The unique key is cleared because it was released at the
terminal transition and may belong to another task now; `startBy` is cleared so that a retried EXPIRED task
does not expire again.

## Expiry and maximum run time

`startBy` is a store time after which a waiting task must not start. Three places enforce it:

- `claim` never returns a task with `startBy <= now`. PostgreSQL filters it in the query
  (`start_by IS NULL OR start_by > now()`); Redis checks `sb` for each popped member and, when it has passed,
  performs the EXPIRED transition inline (state 6, revision +1, index, counters, key release) and continues
  with the next member.
- `MaintenanceStore.expire(limit)` moves QUEUED and SCHEDULED tasks with `startBy <= now` to EXPIRED (revision
  +1, `stateChanged`, unique key handled like a terminal transition) and returns their ids. PostgreSQL uses
  the partial `_start_by` index; Redis keeps the `startby` zset (QUEUED and SCHEDULED ids with an `sb`, scored
  by it), maintained on every transition into and out of a waiting state, so expiry never scans.
- Maintenance runs `expire(limit)` every cycle until a call returns fewer than the limit and emits
  `TaskEvent.Expired(id)` per id. An expiry done inline by a Redis claim emits no event. A task that is claimed
  or already running is not expired.

`TaskClient` rejects a `startBy` before the requested run time.

`maxRun` is enforced in the renewal loop, not in the store. Each pass compares `startedAt + maxRun` of
every running execution with the runner clock; an execution past it gets `requestCancel("max run time <d>
exceeded")`, the same path as an operator's request, including the interrupt for types that opted in. The
task then ends CANCELLED with that reason. The check runs once per renewal period, so a limit is honoured
within a third of `leaseDuration`. `startedAt` is the time the processor started on this runner, not
`claimedAt`.

## Queue policies and pausing

`BackgroundRunner.builder().queue(name, QueuePolicy(weight, maxConcurrent))` (default weight 1,
`maxConcurrent` 0 for unlimited) applies to the runner that has it. A runner without a policy keeps the single
rotated claim over all queues. With any policy, a claim cycle does this, given `slots` free executor slots
and the distinct processed queues in rotated order:

1. For each queue, `cap = min(slots, maxConcurrent - running(queue))` (or `slots` when unlimited, never
   negative). Queues with cap 0 take no part.
2. The share of a queue is `min(cap, slots * weight / sum of weights of the participating queues)`; slots left
   by integer division go one each to queues below their cap in rotated order.
3. One `claim` per queue with a positive share.
4. Slots that a queue could not fill (it returned fewer tasks than its share) are offered once more, in order,
   to queues that did fill their share and still have cap.

So an empty queue does not idle the others, and `running(queue)` is the runner's own count (an
`AtomicInteger` per queue id, decremented when the attempt finishes). Limits are per runner, never fleet-wide:
that needs permit accounting inside `claim` and `ack` and is deferred.

Pausing is a store flag read by `claim`. `AdminStore.pauseQueue(queueId, paused)` writes it (PostgreSQL
`<prefix>_queue.paused`, Redis set `paused`); PostgreSQL leaves paused queue ids out of the claim's queue list
and Redis gives a paused queue a zero budget inside CLAIM. Tasks can still be enqueued, promoted and expired in
a paused queue, and running tasks are not affected. Every runner sees the change on its next claim, which is
within `idlePollInterval` when idle.

## Schedules

The recurring service keeps its one commit, `commitRecurring(due, nextFire, occurrence)`, and adds policy
around it. A definition has a cron expression (evaluated by `Cron.next(expression, after, zone)` in its zone,
UTC when none; a fire time that does not exist in a DST gap moves to the next valid instant, a repeated hour
fires once) or a fixed delay, never both.

**Misfire.** The threshold is `max(2 x recurringTasksCheckInterval, 1 minute)`. A fire time that is late by no
more than that is an ordinary fire. A later one is a misfire, recorded as the issue `schedule-misfire`, and
the definition's policy decides:

- `SKIP`: no task; `setNextFire` to the next fire time computed from now.
- `FIRE_ONCE_NOW` (default): one occurrence with `due = now`, committed with the next fire computed from now.
- `CATCH_UP(max)`: the fire times from the stored one up to now are enumerated. If there are no more than
  `max`, one occurrence is committed for the stored fire time (keeping `fireAt` as its `due`) and the next fire
  is the following fire time, so each later round picks up the next missed one, one per round, until the
  definition has caught up. If there are more than `max`, `setNextFire` moves the definition to the
  `max`-th last missed fire time and an issue is recorded, so only the latest `max` run.

**Fixed delay.** The occurrence is committed with `due = fireAt` and the next fire time parked 100 years ahead
(the same parking as a never-fires cron), and carries the header `bth4j.fixed-delay` holding the delay as an
ISO-8601 duration. When the runner finishes that occurrence, `TaskExecution` looks at the header: on SUCCEEDED
it calls `setNextFire(scheduleId, now + delay)`; on DEAD or CANCELLED it calls
`pauseRecurring(scheduleId, true)` and records the issue `schedule-paused`, so a failing job waits for an
operator. Only an applied outcome acts. The two paths that end an occurrence outside its own execution do the
same pause: maintenance, when an expired lease leaves the occurrence DEAD or finishes a cancel request, and
`AdminOps.cancel`, when it cancels a waiting occurrence at once (both recognise the occurrence by its
`scheduleId` and the `bth4j.fixed-delay` header). An occurrence deleted while waiting leaves the definition
parked until an operator resumes or triggers it. `AdminOps.pauseSchedule(id, false)` resumes and re-arms a
fixed-delay definition at `now + delay`. A new fixed-delay definition first fires one delay after
`putSchedule`.

**Overlap.** An occurrence of a definition with `overlapSkip` carries the unique key `sched:<id>` with
`UNTIL_DONE`. If the previous occurrence still holds the key, the store does not store the new one but the
commit still advances the definition, so the fire time is consumed. The service tells this case from a normal
commit by looking the new id up, records the issue `schedule-overlap` and emits no `ScheduleFired`.

**Pause and trigger.** `pauseRecurring` keeps the definition and its stored next fire time but excludes it from
`dueRecurring` and from `commitRecurring`, so an occurrence cannot be committed for a definition paused after
`dueRecurring` read it. `setNextFire` backs `triggerSchedule` (`next = now`) and the fixed-delay re-arm.
`putRecurring` takes the paused flag from a new definition only; replacing keeps the stored flag, so a runner
that registers its schedules at every start cannot undo an operator's pause. `recurringDefinitions()` returns
each definition with its next fire time for the admin view.

## Workers

Every `start()` registers a worker row (`allocateWorkerId`) with its label. Maintenance calls
`AdminStore.heartbeat(workerId, running)` every `maintenanceCheckInterval`, which writes `lastSeen = now` (store
time) and the number of tasks the runner is running. A worker whose row was purged is not brought back by a
heartbeat (Redis checks that the hash exists). Every purge interval maintenance also calls
`MaintenanceStore.purgeWorkers(workerStaleAfter)` (builder, default 10 minutes), which deletes rows with
`lastSeen < now - staleAfter`. The registry is for operators only; no protocol decision reads it. A runner that
is paused in a debugger or cut off from the store for longer than `workerStaleAfter` loses its row but keeps
running tasks, because leases, not workers, decide ownership.

Redis stores a worker as hash `w:<id>` (`label`, `started`, `seen`, `running`) plus the `workers` zset scored
by `seen`; `purgeWorkers` is a range read below `now - staleAfter`, then `UNLINK` and `ZREM`. PostgreSQL has
`last_seen` and `running integer NOT NULL DEFAULT 0` on `<prefix>_worker` and a `DELETE ... WHERE last_seen <
now() - staleAfter`.

## The admin SPI and operations

`AdminStore` (part of `FullStore`) has the operator calls, all revision-checked where they change a task:
`cancel` (above), `retry` (above), `delete` (any state but LEASED with a matching revision: the row or hash
is removed, the unique key released, the counters corrected), `pauseQueue`, `queues` (id, name, paused),
`heartbeat` and `workers`.

`AdminOps` in core is the application-facing interface over the stores, implemented by
`service.DefaultAdminOps`. It resolves queue names to ids, builds `TaskView`s, adds the schedule view
(`ScheduleView` from `recurringDefinitions`) and, for `pauseSchedule(id, false)`, the re-arm of fixed-delay
definitions. `TaskClient.admin()` returns one without health and issues; `BackgroundRunner.admin()` returns one
that also supplies `health()` and `issues()` of that runner. The reason of a cancel is limited to 512
characters.

Runner state for operators is local to a runner. `RunnerHealth(status, workerId, running, lastClaimAt,
lastMaintenanceAt, lastStoreErrorAt, lastStoreError, issueCount)` is a snapshot, with status NEW, RUNNING,
QUIET or STOPPED. Issues are a bounded ring of 100 `Issue(at, kind, detail, taskId)` kept by `RunnerStatus`;
the kinds are `unknown-event`, `store-error`, `lease-lost`, `retry-policy`, `executor-rejected`,
`schedule-misfire`, `schedule-overlap` and `schedule-paused`. They are in memory and lost on restart.

## Metrics, logging and the HTTP API

None of these change the protocol; they read `TaskEvent`s and `AdminOps`.

`LoggingTaskObserver` (core) writes one SLF4J line per event with `bth4j.task`, `bth4j.event` and `bth4j.queue`
in the MDC. `MicrometerTaskObserver` counts events (`bth4j.tasks` with `event`, `queue`, `result`, `outcome`),
times successful runs and counts deferrals, fires and purges; its tags are queue and event names and results,
never ids or messages, to bound the number of meters. `MicrometerRunnerMetrics.bind` registers gauges for
running tasks, runner up and tasks per state and queue, refreshed by one daemon thread over `admin().counts()`
and `health()`, so a scrape never reads the store.

`AdminHttpServer` (JDK `com.sun.net.httpserver`, virtual thread per request) exposes `AdminOps` under `/api/v1`.
Authentication is a cookie session: `POST /session` checks the password (SHA-256 digests compared with
`MessageDigest.isEqual`), sets `bth4j_session` (`HttpOnly`, `SameSite=Strict`, `Secure` with HTTPS) and returns a
CSRF token that every POST and DELETE must send as `X-CSRF-Token`. Roles are ADMIN and VIEWER. Sessions are in
memory and sliding (12 hours by default). Failed logins are throttled per remote address. The status codes:
401 without a session, 403 for a viewer's mutation or a wrong CSRF token, 404 for an unknown id, 409 for a stale
revision, 429 when throttled. Ids and revisions are JSON strings, since they can exceed 2^53. With `staticDir`,
GET requests outside `/api/` serve files from that directory, unknown paths without an extension fall back to
`index.html` for the single-page app, with `Cache-Control: no-cache` and a content security policy on HTML. CORS
is limited to the configured origins with credentials. The dashboard (`dashboard/`) is a Vue 3 app without a
build step that talks to this API.

## The store SPI and how to implement a store

The SPI is `today.bonfire.oss.bth4j.spi`, split by who calls it; a store module implements all of them
through `FullStore`:

| Interface | Content |
|---|---|
| `TaskStore` | `info`, `init`, `allocateIds`, `allocateWorkerId`, `queueId`, `eventId`, `queueNames`, `eventNames` |
| `ExecutionStore` | `enqueue`, `enqueueAll`, `claim`, `renew`, `ack`, `fail`, `failExpired`, `finishCancelled`, `reschedule`, `unclaim`, `saveSteps`, `progress` |
| `MaintenanceStore` | `promote`, `reclaim`, `expire`, `purge`, `purgeWorkers`, `releaseKeys`, `nextDue` |
| `ScheduleStore` | `putRecurring`, `deleteRecurring`, `recurringSchedules`, `recurringDefinitions`, `dueRecurring`, `commitRecurring`, `pauseRecurring`, `setNextFire` |
| `InspectionStore` | `getTask`, `counts`, `tasks` (by state, keyset paging on `(stateChanged, id)`) |
| `AdminStore` | `cancel`, `retry`, `delete`, `pauseQueue`, `queues`, `heartbeat`, `workers` |

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

- Fleet-wide concurrency limits, rate limits and priority bands. A fleet-wide limit needs permit accounting
  inside `claim` and `ack`, the hot path. `QueuePolicy` limits are per runner.
- Task chains and workflows; use steps inside one task, or enqueue the next task from a processor.
- A push signal from the store to idle runners (see Concurrency model).
- Persistent issues: the runner keeps the last 100 in memory.
