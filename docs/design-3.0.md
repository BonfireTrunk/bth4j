# bth4j 3.0 design

3.0 replaces the coordination protocol of 2.x. A task is always in exactly one state, every
transition between states is atomic together with the ownership check that guards it, and
the whole protocol is expressed against a store interface, so the same services run on any
store that can make those transitions atomic. Nothing in 3.0 can read 2.x data: drain the
2.x queues before upgrading, or use a new namespace.

Modules: `bth4j-core` holds the task model, the runner, the services and the `TaskStore`
interface, with no dependency on a particular store. `bth4j-redis` is the Redis store
(Redis 7.0 or newer). Applications depend on a store module and get core transitively.

## Guarantees

- At least once. A task runs again only when the worker holding it crashed or stopped
  renewing its lease. A slow task never runs twice because it is slow.
- A worker that lost its lease cannot complete, fail or reschedule the task; its calls are
  no-ops. Only the current lease token can move a task.
- Duplicate task ids are rejected at enqueue instead of sharing state.
- One recurring definition produces each occurrence once, also with several instances.
- Shutdown waits for running tasks up to the shutdown timeout. Tasks still running after
  that keep their lease until it expires and are then retried.

## States and transitions

A task is queued, scheduled, leased or dead. Recurring definitions are kept apart; each
occurrence becomes a task of its own.

```
enqueue  --(no due)-->  queued  --claim-->  leased  --ack-->  gone
   |                      ^                   |  |
   +--(due)--> scheduled --promote--+         |  +--fail, attempts left--> scheduled
                  ^                           |  +--fail, exhausted-----> dead (kept 30 d)
                  +-----------reschedule------+  +--lease expired------> reclaim = fail
```

Each arrow is one `TaskStore` call, and the store must make each call atomic together
with its checks:

- `enqueue`: rejects an existing id, then queues the task or schedules it for its due time,
  and signals waiting runners.
- `claim`: takes up to n queued tasks from the given queues in order (the second queue is
  only read when the first could not fill n), increments `attempts`, assigns a token
  nobody else holds and a lease expiry, and returns the tasks with their payload. One call
  for a whole batch.
- `renew`: extends the lease of every task of this instance whose token still matches,
  in one call. Runs every third of the lease duration.
- `ack`: token check, then the task is gone.
- `fail`: token check, then either scheduled at `now + delay` or dead when
  `attempts >= max`. `max` is `Event.retryCount() + 1`, fixed at enqueue time.
- `reschedule` (`TaskRescheduleException`): token check, moves the same task to the
  scheduled state and gives the attempt back.
- `unclaim`: token check, puts the task back at the front of its queue and gives the
  attempt back. Used when the executor rejects a claimed task.
- `promote`: moves due scheduled tasks into their queues, bounded, and returns the next
  due time so the scheduler sleeps exactly as long as needed.
- `reclaim`: returns leased tasks whose lease expired; the caller decides the retry delay
  from `attempts` and calls `fail` with the expired token, which is a no-op if the task
  moved in between.
- `dueRecurring` / `commitRecurring`: the recurring service reads definitions due within
  the queue-ahead window, computes the next fire time with the cached cron parser, and
  commits with a compare on the definition revision and the current fire time. The
  occurrence id is `<sid>:<fireAt>`, so the same occurrence can never be created twice.
  Missed occurrences are skipped: the next fire time is computed from `max(fireAt, now)`.
- `putRecurring` / `deleteRecurring`: register or remove a definition. Re-registering with
  the same expression keeps the next fire time; a changed expression resets it.
- `purgeDeadTasks` and `retryDeadTask`: retention of dead tasks, and putting one back.

There are no locks. Every transition is atomic and idempotent under retry, so instances
simply race. The interface, with the guarantees each method must give, is
`today.bonfire.oss.bth4j.store.TaskStore`.

Times are instants from the client clock. A lease is renewed well before it expires, so
clock skew between instances only matters when it approaches the lease length.

## Pick-up

The processor thread reserves executor slots first, then claims at most that many tasks.
When the claim returns nothing it waits for the store's wake-up signal (a subscription
thread the store creates) with the idle poll interval as fallback. When the executor is
full it waits for a slot, not for a timer. A store without push notifications gives a
subscription that never fires, and the processor polls at the idle interval.

Queues are processed round robin by rotating the queue list on every claim.

## Executor contract

```java
int reserve(int max, Duration wait) throws InterruptedException; // >= 1 or 0 on timeout
void release(int slots);
void execute(Runnable task);   // consumes one reserved slot
void shutdown();
boolean awaitTermination(Duration timeout) throws InterruptedException;
```

`DefaultVtExecutor` keeps its adaptive concurrency limit, now with a configurable upper
bound.

## Public API changes

- The runner takes a `TaskStore` (`BackgroundRunner.Builder.taskStore`) instead of a
  Jedis client and a namespace prefix; `new RedisTaskStore(jedis, "namespace")` is the
  Redis one.
- `Task` is built from the stored record; the pipe-separated task string is gone.
  `accountId` is a `String`. `attempts` and `maxAttempts` are available to processors
  and callbacks.
- `TaskOps.enqueue(Task)` and `enqueue(Task, data)` return `false` for a duplicate id.
  `addRecurringTask`, `deleteRecurringTask(scheduleId)`, `deleteAllRecurringTasks`,
  `getDataForTask`, `queueSize`, counts, `deadTasks`, `retryDeadTask`, `purgeDeadTasks`.
- Builder: `leaseDuration` (default 60 s) replaces `staleTaskTimeout`; `idlePollInterval`
  (1 s) replaces `taskProcessorQueueCheckInterval`; `taskRetryDelay` is
  `Function<Integer, Duration>` with exponential backoff and jitter as default;
  `deadTaskRetention` (30 d); `claimBatchSize` (32). `cronLockDuration` is gone.
- `BackgroundExecutor` has the contract above; `isPoolFull` is gone.
- Payload decoding happens from the claimed record, there is no second read.

## Writing a store

A store implements `TaskStore`. The contract is small because the services never read
and then write in two steps: each method is one atomic transition with its own check.
What a store needs from its engine is a conditional multi-row write (compare the token,
then move the task) and a way to take n queued rows exclusively. On Redis that is one Lua
script per method; on an SQL database it is one statement per method, with
`SELECT ... FOR UPDATE SKIP LOCKED` for the claim and `UPDATE ... WHERE id = ? AND token = ?`
for the token-checked transitions. The wake-up signal maps to pub/sub, `LISTEN/NOTIFY`,
or nothing.

The integration tests of `bth4j-redis` (`TaskOpsIT`, `BackgroundRunnerIT`) exercise the
guarantees through `TaskOps` and the runner and are the reference for a second store.

## Appendix: the Redis store

All keys of a store carry the namespace as a hash tag, so every script touches one slot
and works on Redis Cluster. With namespace `BTH`:

| Key | Type | Content |
|---|---|---|
| `{BTH}:q:<queue>` | list | task ids waiting in `<queue>`; `LPUSH` in, popped from the right |
| `{BTH}:t:<id>` | hash | the task: `event`, `account`, `queue`, `data`, `attempts`, `max`, `created`, `due`, `token`, `lease`, `schedule`, `error`, `died` |
| `{BTH}:scheduled` | zset | id -> due time (ms); delayed tasks and retries waiting for their time |
| `{BTH}:leased` | zset | id -> lease expiry (ms); tasks being executed |
| `{BTH}:dead` | zset | id -> time of death (ms); tasks that exhausted their attempts |
| `{BTH}:cron` | zset | schedule id -> next fire time (ms) |
| `{BTH}:cron:<sid>` | hash | recurring definition: `event`, `account`, `queue`, `cron`, `data`, `max`, `rev` |
| `{BTH}:wake` | channel | queue name published on every push into a queue |

Each `TaskStore` method is one Lua script (`RedisScripts`), called by its SHA-1 with
`EVALSHA` and loaded once with `SCRIPT LOAD` when the server does not know it, for
example after a restart or `SCRIPT FLUSH`. Only the hash and the arguments travel on the
wire. A script runs its reads, comparisons and writes as one unit on the server, so a
token-checked transition is one round trip; the same transition with `WATCH`/`MULTI`/`EXEC`
needs four and a retry loop, and `MULTI`/`EXEC` alone cannot branch on the token at all.
Measured on Redis 7.4, a five-command script costs the server about one and a half plain
commands.

- `claim` uses `LMPOP ... RIGHT COUNT n` per queue and returns every claimed hash with
  `HGETALL`, so the payload comes with the claim.
- `fail` sets an `EXPIRE` on the hash of a dead task equal to the dead task retention, so
  storage stays bounded even if no maintenance service runs; the dead set entry is removed
  by `purgeDeadTasks`.
- `promote` uses `ZRANGE ... BYSCORE LIMIT` and publishes on the wake channel when it
  moved something.

Requires Redis 7.0 or newer (`LMPOP`, `ZRANGE ... BYSCORE LIMIT`, scripts).
