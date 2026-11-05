# bth4j-micrometer

Micrometer metrics for bth4j runners. `micrometer-core` is a provided dependency: bring your own version.

## Meters

Written by `MicrometerTaskObserver` (one per runner, driven by task events):

| Meter | Type | Tags | Notes |
|---|---|---|---|
| `bth4j.tasks` | counter | `type`, `queue`, `result`, `outcome` | `result` is `succeeded`, `retried`, `died`, `cancelled`, `rescheduled`, `expired` or `lease_lost`. `outcome` is `applied`, `lost` when the lease was lost before the store applied the transition, or `unknown` when the store call threw and the transition may or may not have been applied. `expired` and `lease_lost` come from maintenance and carry only ids, so their `type` and `queue` are `unknown`. |
| `bth4j.task.duration` | timer | `type`, `queue` | Run time of succeeded tasks. |
| `bth4j.deferred` | counter | `type` | Claimed tasks that could not run (no processor, undecodable payload) and were put back. |
| `bth4j.schedule.fired` | counter | none | Occurrences created from recurring definitions. |
| `bth4j.purged` | counter | `state` | Finished tasks removed after their retention, by state. |

Written by `MicrometerRunnerMetrics` (gauges, refreshed by one daemon thread):

| Meter | Type | Tags | Notes |
|---|---|---|---|
| `bth4j.runner.concurrency.limit` | gauge | none | Current executor limit, registered when available through `bind(runner, ...)`; removed on close. |
| `bth4j.runner.running` | gauge | none | Tasks running in this runner right now. |
| `bth4j.runner.abandoned` | gauge | none | Attempts past their maximum run time that were abandoned and still hold a slot. |
| `bth4j.runner.up` | gauge | none | 1 while the runner health status is `RUNNING`, otherwise 0. |
| `bth4j.tasks.state` | gauge | `state`, `queue` | Tasks per state and queue as of the last refresh. A queue that disappears drops to 0. |

Any common tags you pass are added to every meter. Tags only ever hold type, queue, state, result and outcome
names; task ids and messages are never used, so the number of meters is bounded.

## Wiring

```java
MeterRegistry registry = ...;

BackgroundRunner runner = BackgroundRunner.builder()
        .store(store)
        .registry(processors)
        .observer(new MicrometerTaskObserver(registry, Tags.of("app", "billing")))
        .build();
runner.start();

MicrometerRunnerMetrics metrics =
        MicrometerRunnerMetrics.bind(runner, registry, Duration.ofSeconds(15));

// on shutdown
metrics.close();
runner.stop();
```

The runner overload also binds its concurrency limit, with an optional common-tags overload. The overloads
taking `AdminOps` and a running-task supplier do not expose a concurrency limit. `admin` is the runner's
`AdminOps` (the one that reports the runner's health). The poller reads `admin.counts()`
and `admin.health()` every refresh interval, so scraping never queries the store. If reading the counts fails, the last
counts stay; if reading health fails, `bth4j.runner.up` becomes 0.
