# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.0] - 2026-10-31

3.0 is a redesign of 2.x: a new public API, a new coordination protocol and a storage interface with Redis,
PostgreSQL and in-memory implementations. 3.0 cannot read 2.x data: drain the 2.x queues before upgrading,
or start 3.0 with a new Redis namespace. Requires Java 21, and Redis 7.0 or newer or PostgreSQL 9.5 or
newer, depending on the store. See `docs/design-3.0.md`.

### Changed (breaking)

- The single `bth4j` artifact is split into `bth4j-core`, `bth4j-redis` and `bth4j-postgres`, with `bth4j-testing` for tests. Applications depend on `bth4j-core` and one store module.
- Task types: `Event` (and the `eventParser` setting) is replaced by `TaskType<T>`, declared once per kind of task with `TaskType.of(name, PayloadClass.class)`, `TaskType.of(name, PayloadCodec)` or `TaskType.of(name)` and `payloadVersion(n)`. The same constant is used to enqueue and to register the processor.
- Producers: `TaskOps` is replaced by `TaskClient`, reached with `BackgroundRunner.client()` or built on its own with `TaskClient.builder()`. `Task` and `Task.Builder` are replaced by the immutable `TaskRequest` (queue, account, `at`, `delay`, `uniqueKey`, `maxAttempts`, headers, `id`, plus `startBy`, `maxRun` and `retryPolicy`). `enqueue` returns `EnqueueResult.Accepted` or `Duplicate` instead of a boolean, and throws `EnqueueFailedException` (carrying the id to retry with) when the store cannot be reached. Recurring definitions are `ScheduleDefinition` with `putSchedule`, `deleteSchedule` and `schedules`.
- Processors: `TaskProcessor<T>` is a functional interface `process(TaskContext, T)`; `dataTypeClass()` is gone. `TaskContext` gives the task, id, attempt, headers, clock, `progress`, `step` and the cancel flag. `TaskCallbacks` and `taskHandlerDecorators` / `recurringTaskHandlerDecorators` are replaced by `TaskObserver` and the sealed `TaskEvent` (`Started`, `Succeeded`, `Retrying`, `Died`, `Rescheduled`, `Cancelled`, `Expired`, `LeaseExpired`, `Deferred`, `ScheduleFired`, `Purged`).
- Task ids are 64-bit numbers allocated by the client in blocks (`TaskClient.nextId()`, `idBlockSize`); the text ids with the `|` separator and the pipe-separated task string are gone. Queue names and task type names are stored as numeric ids from registries in the store, and there are no foreign keys.
- The coordination protocol is lease and revision based. Every state change is one atomic store call that checks the task's `revision` (the fence); a worker that lost its lease cannot complete, fail, reschedule or checkpoint the task. The stale-task timeout, the lock-based maintenance and the lease token are gone: `leaseDuration` replaces `staleTaskTimeout`, and an expired lease is retried by `failExpired`, which also leaves a lease alone that was renewed after the expiry scan.
- Task states are QUEUED, SCHEDULED, LEASED, SUCCEEDED, DEAD, CANCELLED and EXPIRED. Succeeded tasks are kept (10 minutes by default) instead of removed, dead tasks are kept 90 days by default, and a retention of zero removes the record at the transition. A task ends CANCELLED when an operator cancels it, when its processor throws `TaskCancelledException` or when it runs past `maxRun`, and EXPIRED when its `startBy` passes before it started; both are kept for `cancelledRetention` (48 hours by default).
- Failures are recorded on the task (the last 20, with type, message and stack). Retries follow a `RetryPolicy` (exponential backoff with jitter by default) instead of `taskRetryDelay` and `Event.retryCount()`; the attempts are `maxAttempts` per request, `defaultMaxAttempts` (10) otherwise.
- `BackgroundRunner`: `BackgroundRunner.builder()` replaces `new BackgroundRunner.Builder()`, `start()` replaces `run()` and `stop()` replaces `stopRunner()`; new `quiet()`, `resume()`, `registry()`, `client()` and `workerId()`. The builder takes `store(FullStore)` instead of `jedisClient` and `namespacePrefix`, `concurrency(Concurrency)` instead of choosing an executor, and its settings were renamed or added: `idlePollInterval` replaces `taskProcessorQueueCheckInterval`, `scheduledTasksCheckInterval` replaces `delayedTasksQueueCheckInterval`, `serviceCheckInterval` replaces `BGThreadCheckInterval`, `succeededRetention`, `deadRetention`, `cancelledRetention`, `keepSucceededPayload`, `purgeInterval`, `purgeBatchSize`, `claimBatchSize`, `idBlockSize`, `defaultMaxAttempts`, `workerLabel`, `observer`, `retryPolicy`, `clock`, plus `retryPolicy(name, policy)`, `queue(name, QueuePolicy)` and `workerStaleAfter`. `cronLockDuration` and `recurringTasksQueueAheadDuration` are gone; occurrences are created when they come due.
- Concurrency: the default is a fixed limit of twice the number of processors on virtual threads; `Concurrency.adaptive(min, max)` keeps the adaptive limit. `DefaultPtExecutor` is removed. `BackgroundExecutor` has the contract `reserve(max, wait)`, `release(n)`, `execute`, `activeCount`, `shutdown`, `awaitTermination`; `isPoolFull` and `getExecutor` are gone.
- Unknown task types: a runner claims only the types it has processors for (PostgreSQL filters in the claim query, Redis defers them 5 seconds without an attempt, the in-memory store skips them).
- Recurring definitions: each fire time creates one task exactly once across instances (the commit compares the definition revision and the fire time); missed fire times are skipped; re-registering with the same expression keeps the next fire time; definition revisions come from a counter that never resets.
- `stop()` waits for services and running tasks up to `shutdownTimeout` (default 30 seconds), keeps renewing the leases of running tasks meanwhile, and shuts down the task executor.
- The Redis key layout lives in `bth4j-redis` (`THC` is gone). All keys carry the namespace as a hash tag, so the store works on Redis Cluster, and every transition is one Lua script that reads Redis time.
- `QueuesHolder` holds logical queue names; the default queues are `Q:DEF`, `Q:LOW`, `Q:MED` and `Q:HIGH` with `Q:DEF` as default.

### Added

- `bth4j-core`: `TaskType`, `PayloadCodec`, `TaskRequest`, `Uniqueness`, `EnqueueResult`, `TaskClient`, `TaskProcessor`, `TaskContext`, `TaskView`, `TaskProcessorRegistry`, `RetryPolicy`, `TaskObserver`, `TaskEvent`, `Concurrency`, `ScheduleDefinition`, and the store SPI in `today.bonfire.oss.bth4j.spi` (`TaskStore`, `ExecutionStore`, `MaintenanceStore`, `ScheduleStore`, `InspectionStore`, `FullStore`, `TaskRecord`, `TaskState`).
- `bth4j-redis`: `RedisTaskStore(UnifiedJedis, namespace)`: one hash per task with only the fields that are set, sorted sets for queues and state indexes, one Lua script per transition. Requires Redis 7.0 or newer because the scripts read the clock before writing (effect replication).
- `bth4j-postgres`: `PostgresTaskStore(DataSource, prefix)`: tables `<prefix>_task`, `_queue`, `_event`, `_worker`, `_schedule` and `_meta`, partial indexes per state, claims with one bounded index scan per queue under `FOR UPDATE SKIP LOCKED`, revision-checked `UPDATE` statements. `init()` creates the schema under an advisory lock and `schemaStatements()` provides the DDL for migration tools. Uses only `java.sql`. The SQL uses `FOR UPDATE SKIP LOCKED` and `ON CONFLICT`, so PostgreSQL 9.5 or newer is required; it is tested on 17.
- `bth4j-testing`: `InMemoryStore` and `MutableClock` for tests of applications.
- `bth4j-store-tests`: `StoreContract`, the tests every store must pass; all three stores run it. Not published.
- Steps: `TaskContext.step(name, ...)` stores the result of each step durably, so a retry resumes after the last completed step. At least once per step; side effects belong inside steps.
- Unique keys with a keep window (`Uniqueness.keepFor(duration)`), enforced by a partial unique index in PostgreSQL and a key string in Redis.
- Client-allocated ids and safe enqueue retries after a lost reply (`TaskClient.nextId()`, `TaskRequest.Builder.id`).
- Progress text for operators (`TaskContext.progress`), account labels and headers on tasks, a worker registry with a label per runner.
- Retention and purge per terminal state, with unique-key release; `quiet()` and `resume()` on the runner.
- Inspection on the client: `task(id)`, `counts()` and `tasks(state, queue, afterId, limit)`.
- Builder settings are validated, and `build()` fails early on a missing store or an invalid queue configuration.
- GitHub Actions workflows for pull request builds, CLA checks and releases, using the shared BonfireTrunk workflows.
- `CHANGELOG.md` and a Contributing section in the README.
- Cancel: `AdminOps.cancel(id, revision, reason)` cancels a QUEUED or SCHEDULED task at once and asks a LEASED task to stop; the runner learns of it when it renews the lease, `TaskContext.cancelRequested()` and `checkCancelled()` show it to the processor, and the task ends CANCELLED with the reason kept on the record. `TaskType.interruptOnCancel()` also interrupts the processor's thread.
- `startBy` is enforced: a QUEUED or SCHEDULED task whose `startBy` passed is never claimed, maintenance moves it to EXPIRED and the runner emits `TaskEvent.Expired`.
- `maxRun` is enforced: a task running longer than `maxRun` gets a cancel request ("max run time ... exceeded") from the lease renewal and ends CANCELLED.
- Named retry policies: `BackgroundRunner.builder().retryPolicy(name, policy)` registers one and `TaskRequest.retryPolicy(name)` picks it; an unknown name uses the default policy and records an issue.
- Queue policies: `BackgroundRunner.builder().queue(name, QueuePolicy)` gives a queue a `weight` for its share of the free slots and a `maxConcurrent` limit on this runner.
- Queue pause and resume: `AdminOps.pauseQueue(queue, paused)` stops every runner from claiming from the queue; tasks can still be enqueued to it.
- Health: `BackgroundRunner.health()` returns a `RunnerHealth`, `issues()` returns the last 100 `Issue`s the runner recorded, and `drain(Duration)` goes quiet and waits for the running tasks.
- Worker registry: runners write a heartbeat (`lastSeen`, `running`) every `maintenanceCheckInterval`, and maintenance deletes workers silent for `workerStaleAfter` (10 minutes by default).
- `AdminOps`, the operator view and operations over one store (counts, queues, tasks, retry, cancel, delete, workers, schedules, health, issues), reached with `BackgroundRunner.admin()` or `TaskClient.admin()`; the SPI has the new `AdminStore` in `FullStore`.
- `TaskClient.prepare(TaskRequest)` returns the validated `TaskRecord`, and `PostgresTaskStore.enqueue(Connection, TaskRecord)` stores it inside the caller's transaction.
- Schedules: fixed-delay definitions (`ScheduleDefinition.every(id, type, payload, delay)`, re-armed when an occurrence succeeds and paused when an occurrence ends dead or cancelled), `zone(ZoneId)` for cron, `misfire(Misfire.SKIP | FIRE_ONCE_NOW | catchUp(max))`, `overlap(Overlap.SKIP)`, and pause, resume and trigger through `AdminOps`.
- `LoggingTaskObserver`: one SLF4J line per `TaskEvent` with `bth4j.task`, `bth4j.event` and `bth4j.queue` in the MDC.
- `bth4j-micrometer`: `MicrometerTaskObserver` (counters `bth4j.tasks`, `bth4j.deferred`, `bth4j.schedule.fired`, `bth4j.purged` and timer `bth4j.task.duration`) and `MicrometerRunnerMetrics.bind` (gauges `bth4j.runner.running`, `bth4j.runner.up` and `bth4j.tasks.state`); `micrometer-core` is a provided dependency.
- `bth4j-admin-http`: `AdminHttpServer`, a JSON admin API over `AdminOps` on the JDK HTTP server with users, roles (`ADMIN`, `VIEWER`), cookie sessions, CSRF tokens, optional CORS and HTTPS, and static file serving.
- `dashboard/`: a Vue 3 operator dashboard without a build step (overview, queues, tasks, dead tasks, schedules, workers, issues), served by `AdminHttpServer.staticDir` or any static host.
- Redis: the keys `paused`, `startby`, `workers` and `crondefs`, the fields `seen` and `running` on `w:<id>`, the fields `zone`, `mf`, `cum`, `fd`, `os` and `paused` on `cron:<id>`, and the scripts CANCEL, FINISH_CANCELLED, RETRY, DELETE, EXPIRE, PURGE_WORKERS, CRON_PAUSE and CRON_SET_NEXT. `reclaim`, `dueRecurring` and `heartbeat` are plain commands (24 scripts in all), and the reverse registry hashes `reg:*:names` and the `queues` set are gone: listings invert `reg:queue` and `reg:event`, purge walks the values of `reg:queue`.
- PostgreSQL: `<prefix>_worker.running`, and `<prefix>_schedule` columns `zone`, `misfire`, `catch_up_max`, `fixed_delay_ms`, `overlap_skip` and `paused` with the partial index `_due (next_fire) WHERE NOT paused`; the schema version stays 1.

### Removed

- `Event`, `Task`, `Task.Builder`, `TaskOps`, `TaskCallbacks`, and the builder settings `jedisClient`, `namespacePrefix`, `eventParser`, `taskHandlerDecorators`, `recurringTaskHandlerDecorators`, `staleTaskTimeout`, `taskRetryDelay`, `cronLockDuration` and `recurringTasksQueueAheadDuration`.
- `DefaultPtExecutor`, `THC`, and the single `bth4j` artifact.
- Text task ids, the pipe-separated task string and the locks of the stale/lock based protocol.
- Reading 2.x data: queues of a 2.x deployment must be drained before upgrading.

### Changed

- Jedis moved to 8.0.2. `JedisPooled` no longer exists in Jedis 8, so `RedisTaskStore` takes a `UnifiedJedis`, for example a `RedisClient` built with `RedisClient.builder()`.
- Parent POM moved to bonfire-oss-parent 2.1.1. All sources are formatted with palantir-java-format through Spotless, which CI enforces.
- Integration tests of the store modules start their own Redis or PostgreSQL with Testcontainers and run under maven-failsafe-plugin in the `integration-test` phase; they need Docker.
- Maven Wrapper upgraded to 3.3.4 with Maven 3.10.0 and a pinned distribution checksum.
- Lombok removed; the library has no annotation processor any more.
