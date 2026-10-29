# bth4j 3.0 design

3.0 replaces the coordination protocol of 2.x. Every state transition of a task is one
atomic Redis script that checks ownership before it writes. Nothing in 3.0 can read 2.x
data: drain the 2.x queues before upgrading, or use a new namespace.

Requires Redis 7.0 or newer (`LMPOP`, `ZRANGE ... BYSCORE LIMIT`, scripts).

## Guarantees

- At least once. A task runs again only when the worker holding it crashed or stopped
  renewing its lease. A slow task never runs twice because it is slow.
- A worker that lost its lease cannot complete, fail or reschedule the task; its calls are
  no-ops. Only the current lease token can move a task.
- Duplicate task ids are rejected at enqueue instead of sharing state.
- One recurring definition produces each occurrence once, also with several instances.
- Shutdown waits for running tasks up to the shutdown timeout. Tasks still running after
  that keep their lease until it expires and are then retried.

## Keys

All keys of a runner carry the namespace as a hash tag, so every script touches one slot
and works on Redis Cluster. With namespace `BTH`:

| Key | Type | Content |
|---|---|---|
| `{BTH}:q:<queue>` | list | task ids waiting in `<queue>`; `LPUSH` in, popped from the right |
| `{BTH}:t:<id>` | hash | the task: `event`, `account`, `queue`, `data`, `attempts`, `max`, `created`, `due`, `token`, `lease`, `schedule`, `error`, `died` |
| `{BTH}:scheduled` | zset | id -> due time (ms); delayed tasks and retries waiting for their time |
| `{BTH}:leased` | zset | id -> lease expiry (ms); tasks being executed |
| `{BTH}:dead` | zset | id -> time of death (ms); tasks that exhausted their attempts |
| `{BTH}:cron` | zset | schedule id -> next fire time (ms) |
| `{BTH}:cron:<sid>` | hash | recurring definition: `event`, `account`, `queue`, `cron`, `data`, `rev` |
| `{BTH}:wake` | channel | queue name published on every push into a queue |

Times are epoch milliseconds from the client clock. A lease is renewed well before it
expires, so clock skew between instances only matters when it approaches the lease length.

## Task life

```
enqueue  --(no due)-->  q:<queue>  --claim-->  leased  --ack-->  gone
   |                       ^                     |  |
   +--(due)--> scheduled --promote--+            |  +--fail, attempts left--> scheduled
                  ^                              |  +--fail, exhausted-----> dead (hash kept 30 d)
                  +--------------reschedule------+  +--lease expired------> reclaim = fail
```

Each arrow is one script:

- `enqueue`: rejects an existing id, writes the hash, then `LPUSH` into the queue or `ZADD`
  into `scheduled`, and publishes on `wake`.
- `claim`: for the given queues in order, `LMPOP ... RIGHT COUNT n` up to the number of
  executor slots the caller reserved, increments `attempts`, writes `token` and `lease`,
  adds to `leased`, and returns the whole hash of every claimed task. One round trip for a
  whole batch, and the payload comes with it.
- `renew`: extends the lease of every task of this instance whose token still matches,
  in one call. Runs every third of the lease duration.
- `ack`: token check, then removes from `leased` and unlinks the hash.
- `fail`: token check, then either `scheduled` at `now + delay` or `dead` when
  `attempts >= max`. `max` is `Event.retryCount() + 1`, fixed at enqueue time.
- `reschedule` (`TaskRescheduleException`): token check, moves the same task to
  `scheduled` and gives the attempt back.
- `unclaim`: token check, puts the id back at the front of its queue and gives the
  attempt back. Used when the executor rejects a claimed task.
- `promote`: moves due ids from `scheduled` into their queues, bounded, and returns the
  next due time so the scheduler sleeps exactly as long as needed.
- `reclaim`: returns expired entries of `leased`; the caller decides the retry delay from
  `attempts` and calls `fail` with the expired token, which is a no-op if the task moved
  in between.
- `cronDue` / `cronCommit`: the recurring service reads definitions due within the
  queue-ahead window, computes the next fire time with the cached cron parser, and commits
  with a compare on the definition revision and the current fire time. The occurrence id is
  `<sid>:<fireAt>`, so the same occurrence can never be created twice. Missed occurrences
  are skipped: the next fire time is computed from `max(fireAt, now)`.
- `cronPut` / `cronDelete`: register or remove a definition. Re-registering with the same
  expression keeps the next fire time; a changed expression resets it.
- `deadPurge` and `retryDead`: retention of the dead set, and putting a dead task back.

There are no locks. Every script is atomic and idempotent under retry, so instances
simply race.

## Pick-up

The processor thread reserves executor slots first, then claims at most that many tasks.
When the claim returns nothing it waits on the `wake` channel (a dedicated subscription
thread) with the idle poll interval as fallback. When the executor is full it waits for a
slot, not for a timer.

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

- `Task` is built from the hash; the pipe-separated task string is gone. `accountId` is a
  `String`. `attempts` and `maxAttempts` are available to processors and callbacks.
- `TaskOps.enqueue(Task)` and `enqueue(Task, data)` return `false` for a duplicate id.
  `addRecurringTask`, `deleteRecurringTask(scheduleId)`, `deleteAllRecurringTasks`,
  `getDataForTask`, `queueSize`, counts, `deadTasks`, `retryDeadTask`, `purgeDeadTasks`.
- Builder: `leaseDuration` (default 60 s) replaces `staleTaskTimeout`; `idlePollInterval`
  (1 s) replaces `taskProcessorQueueCheckInterval`; `taskRetryDelay` is
  `Function<Integer, Duration>` with exponential backoff and jitter as default;
  `deadTaskRetention` (30 d); `claimBatchSize` (32). `cronLockDuration` is gone.
- `BackgroundExecutor` has the contract above; `isPoolFull` is gone.
- Payload decoding happens from the claimed hash, there is no second read.
