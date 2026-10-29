# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

- Parent POM moved to bonfire-oss-parent 2.1.1. Lombok is now declared by this project, and all sources are formatted with palantir-java-format through Spotless, which CI enforces.
- The Redis integration test now starts its own Redis with Testcontainers and runs under maven-failsafe-plugin in the `integration-test` phase. It no longer expects a Redis on a fixed local port.
- Maven Wrapper upgraded to 3.3.4 with Maven 3.10.0 and a pinned distribution checksum.
