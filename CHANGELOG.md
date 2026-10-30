# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This release replaces the coordination protocol of 2.x. 3.0 cannot read 2.x queues: drain them before
upgrading, or start 3.0 with a new namespace. Redis 7.0 or newer is required. See `docs/design-3.0.md`.

### Changed (3.0, breaking)

- The library is split into `bth4j-core` (task model, runner, services, the `TaskStore` interface), `bth4j-redis` (the Redis store) and `bth4j-postgres` (the PostgreSQL store). Applications depend on a store module; the single `bth4j` artifact is gone.
- Storage is behind `today.bonfire.oss.bth4j.store.TaskStore`: enqueue, claim and the token-checked transitions, each of which a store must make atomic. `BackgroundRunner.Builder.taskStore(TaskStore)` replaces `jedisClient` and `namespacePrefix`; the Redis store is `new RedisTaskStore(jedis, "namespace")`. `TaskOps` takes a store instead of a Jedis client. A store on another engine implements the same interface.
- Every task state transition in the Redis store is one atomic Lua script that checks the task's lease token. A worker that lost its lease cannot complete, fail or reschedule the task any more. Failed tasks are retried immediately after the configured delay instead of after the stale timeout, and a task that runs longer than the lease keeps renewing it instead of being executed twice.
- Tasks are stored as one hash per id and queues hold ids. The pipe-separated task string is gone, duplicate ids are rejected by `TaskOps.enqueue`, and `Task` exposes `attempts()`, `maxAttempts()`, `scheduleId()`, `data()` and `lastError()`. `accountId()` is a `String`.
- All keys carry the namespace as a hash tag (`{BTH}:...`), so the library works on Redis Cluster.
- The task processor reserves executor slots first, then claims up to that many tasks from all queues in one call, and sleeps for `idlePollInterval` (default 500 ms) only when that call found nothing. There are no locks anywhere.
- `TaskOps`: `enqueue(Task)` and `enqueue(Task, data)` replace `addTaskToQueue`; `addRecurringTask(Task[, data])` returns the schedule id, `deleteRecurringTask(String)` takes it; new `getTask`, `decodeData`, `recurringTasks`, `leasedCount`, `scheduledCount`, `deadCount`, `deadTasks`, `retryDeadTask` and `purgeDeadTasks`.
- Recurring definitions produce each occurrence exactly once across instances (`<scheduleId>:<fireAt>` ids); missed occurrences are skipped; re-registering with the same cron expression keeps the next fire time; the data given to `addRecurringTask` is passed to every occurrence.
- `BackgroundExecutor` contract: `reserve(max, wait)`, `release(n)`, `execute`, `activeCount`, `shutdown`, `awaitTermination`. `isPoolFull` and `getExecutor` are gone. `DefaultVtExecutor` has a configurable upper bound for its adaptive limit.
- `BackgroundRunner.Builder`: `leaseDuration` replaces `staleTaskTimeout`, `idlePollInterval` replaces `taskProcessorQueueCheckInterval`, `scheduledTasksCheckInterval` replaces `delayedTasksQueueCheckInterval`, `serviceCheckInterval` replaces `BGThreadCheckInterval`, `recurringTasksQueueAheadDuration` and `recurringTasksCheckInterval` are renamed accordingly, `taskRetryDelay` takes a `Function<Integer, Duration>` with exponential backoff and jitter as default, new `deadTaskRetention` and `claimBatchSize`; `cronLockDuration` is gone. `build()` no longer stores the default executor in the builder.
- `stopRunner()` keeps renewing the leases of running tasks until they finish or the shutdown timeout elapses.
- `THC` is gone; the Redis key layout lives in `bth4j-redis`. `QueuesHolder` holds logical queue names.

### Added

- `bth4j-postgres`: `PostgresTaskStore(DataSource, prefix)` keeps tasks in `<prefix>_task` and `<prefix>_schedule`, claims with `FOR UPDATE SKIP LOCKED` and checks lease tokens in single `UPDATE` statements. `createSchema()` and `schemaStatements()` provide the DDL. Uses only `java.sql`; needs PostgreSQL 9.5 or newer and the application's driver.
- `bth4j-store-tests`: the contract tests (`TaskOpsContract`, `BackgroundRunnerContract`, `ThroughputContract`) that every store must pass; both store modules run them.
- `BackgroundRunner.Builder.shutdownTimeout(Duration)`: how long `stopRunner()` waits for the services and for running tasks (default 30 seconds). `stopRunner()` now also shuts down the task executor and waits for running tasks before returning.
- `BackgroundExecutor.awaitTermination(Duration)` default method and `CustomThread.waitTillDone(Duration)`.
- `TaskRescheduleException(long, String, Throwable)` constructor; a negative delay is rejected.
- `TaskCallbacks` is now public, so applications can build the callbacks that the builder already accepted.
- Builder settings are validated: durations must be positive, `build()` fails early on a missing event parser or Jedis client and on an invalid queue configuration, and `cronLockDuration` returns the builder for chaining.
- Task validation: the `|` separator is rejected in ids, account ids and queue names, cron expressions are validated when the task is built, and a recurring event requires a cron expression unless an execution time is set.
- Unit tests for the builder, task, queue holder, executors, custom thread, task operations and runner wrapper.
- GitHub Actions workflows for pull request builds, CLA checks and releases, using the shared BonfireTrunk workflows.
- `CHANGELOG.md` and a Contributing section in the README.

### Fixed

- Default retry delay used XOR (`retryCount ^ 3`) instead of the cube of the retry count.
- `Task.Builder.newTask(uniqueId)` ignored the given id and generated a new one.
- Data set with `Task.setData(Object)` was dropped when `TaskOps.addTaskToQueue(task, null)` was called.
- The default queue depended on `Set` iteration order; the default queues are now kept in insertion order with `Q:DEF` first.
- `TaskOps.getDataForTask(String, Class)` threw a `TaskDataException` for a missing key instead of returning null, and lost the cause.
- Service threads could miss a stop request: the stop flag was not volatile and sleeping threads were not interrupted.
- Callbacks ran before the task state was applied, and an exception thrown by a callback changed the outcome of the task. The state change now happens first and callback errors are only logged.
- `DefaultPtExecutor.isPoolFull()` could never report full because the executor queue is unbounded; it now counts active and queued tasks.
- `DefaultVtExecutor` counted completed tasks with a non-atomic counter, blocked the monitor thread when shrinking permits, and leaked a permit when the executor rejected a task.
- Locks held by the maintenance, scheduled and recurring services were not released when an exception occurred.
- One bad recurring task entry aborted the whole recurring scan; the entry is now skipped and logged.
- The maintenance service kept the previous temp rotation list for ever, so its memory use grew with every cycle.
- `BackgroundRunner.run()` and `stopRunner()` were not safe against repeated or concurrent calls.

### Changed

- Jedis moved to 8.0.2 (with simple-object-pool 3.0.0). `JedisPooled` no longer exists in Jedis 8, so `RedisTaskStore` takes a `UnifiedJedis`, for example a `RedisClient` built with `RedisClient.builder()`. Pool settings use `ConnectionPoolConfig.builder()`.
- Parent POM moved to bonfire-oss-parent 2.1.1. All sources are formatted with palantir-java-format through Spotless, which CI enforces.
- The Redis integration test now starts its own Redis with Testcontainers and runs under maven-failsafe-plugin in the `integration-test` phase. It no longer expects a Redis on a fixed local port.
- Maven Wrapper upgraded to 3.3.4 with Maven 3.10.0 and a pinned distribution checksum.
- Lombok removed; the library has no annotation processor any more. Public accessors are unchanged.
