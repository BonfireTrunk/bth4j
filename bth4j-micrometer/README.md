# bth4j-micrometer

Micrometer metrics for bth4j runners. `micrometer-core` is a provided dependency: bring your own version.

## Meters

Written by `MicrometerTaskObserver` (one per runner, driven by task events):

| Meter | Type | Tags | Notes |
|---|---|---|---|
| `bth4j.tasks` | counter | `event`, `queue`, `result`, `outcome` | `result` is `succeeded`, `retried`, `died`, `cancelled`, `rescheduled` or `lease_lost`. `outcome` is `applied`, or `lost` when the lease was lost before the store applied the transition. `lease_lost` comes from maintenance and carries only ids, so its `event` and `queue` are `unknown`. |
| `bth4j.task.duration` | timer | `event`, `queue` | Run time of succeeded tasks. |
| `bth4j.deferred` | counter | `event` | Claimed tasks that could not run (no processor, undecodable payload) and were put back. |
| `bth4j.schedule.fired` | counter | none | Occurrences created from recurring definitions. |
| `bth4j.purged` | counter | `state` | Finished tasks removed after their retention, by state. |

Written by `MicrometerRunnerMetrics` (gauges, refreshed by one daemon thread):

| Meter | Type | Tags | Notes |
|---|---|---|---|
| `bth4j.runner.running` | gauge | none | Tasks running in this runner right now. |
| `bth4j.runner.up` | gauge | none | 1 while the runner health status is `RUNNING`, otherwise 0. |
| `bth4j.tasks.state` | gauge | `state`, `queue` | Tasks per state and queue as of the last refresh. A queue that disappears drops to 0. |

Any common tags you pass are added to every meter. Tags only ever hold queue, event, state, result and outcome
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
        MicrometerRunnerMetrics.bind(admin, runner::runningTasks, registry, Duration.ofSeconds(15));

// on shutdown
metrics.close();
runner.stop();
```

`admin` is the runner's `AdminOps` (the one that reports the runner's health). The poller reads `admin.counts()`
and `admin.health()` every refresh interval, so scraping never queries the store. If reading the counts fails, the last
counts stay; if reading health fails, `bth4j.runner.up` becomes 0.
