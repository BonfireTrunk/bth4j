# Fault-tolerance tests

bth4j promises that a task is stored once per id and is never lost, also when a store call fails: it runs, or it ends in a state that says why. A claim whose answer is lost has used up one attempt, so a task whose last attempt went that way ends `DEAD` without having run (with `maxAttempts(1)` that is the first claim). The fault tests check this on every store by cutting one store call at a chosen point, either before the server did the work or after it, and then looking at what the call left behind and what a resend answers.

On this page:

- [What is tested](#what-is-tested)
- [Where a call is cut](#where-a-call-is-cut)
- [What a lost answer leaves behind](#what-a-lost-answer-leaves-behind)
- [Running the tests](#running-the-tests)
- [Running them against another store](#running-them-against-another-store)

## What is tested

| Level | Class in `bth4j-store-tests` | What it does |
|---|---|---|
| Store | `FaultContract` | For every write of the store interface, cuts the call at every fault point, before and after, and checks three rules. A call with more than ten fault points is cut at six of them: the first two, the last two, and the points at one third and at two thirds. |
| Runner | `RunnerFaultContract` | Runs a `BackgroundRunner` and a `TaskClient` on a store whose calls fail at random, through a renewal outage, through a runner that loses the store for good, and through a lost enqueue reply that the caller sends again. |

The three rules of the store level, checked after each cut:

| Rule | What it means |
|---|---|
| Throws | The call throws `TaskStoreException`. |
| Atomic | The stored state equals the state before the call or the state after a clean call, never something in between. A call that works on many tasks at once (`enqueueAll`, `renew`, `ackAll`) is atomic per task, and the test checks that every task is either done or not. |
| Convergent | The same call sent again gives the answer the interface documents for a resend, and the state then equals the state after one clean call. |

Times, and the revision of a schedule, are compared by presence only. PostgreSQL takes schedule revisions from a sequence, so a cut that rolls back leaves a gap, which does no harm.

## Where a call is cut

| Store | Fault point | What a cut does |
|---|---|---|
| PostgreSQL | Each statement execution and each commit of the JDBC driver | Before: the statement never runs. After: it ran and the caller gets the exception. Either way the connection is aborted and closed, so the open transaction rolls back. |
| SQLite | The same, through the SQLite JDBC driver | The same. |
| Redis and Valkey | Each command that Jedis sends, seen by a small TCP proxy that parses the protocol | Before: the proxy closes both sockets without forwarding. After: it forwards, reads the whole reply, drops it and closes both sockets. A pipeline of several commands has several points and can be cut part way. |
| In-memory | Each call of the store | Before: the call does not run. After: it ran and the answer is dropped. |

The connection pool sits on top of the cutting layer, so a cut connection leaves the pool the way a real broken connection does.

The Jedis client of the tests does not retry. A client that retries a lost command on its own changes the answers:

| Call | With a retry inside the client |
|---|---|
| `enqueue` | Answers stored and keeps one task, because the retry carries the same write token. |
| `ack` | Answers false although it applied. |
| `claim` | The first claim stays leased and unseen until its lease expires. |

## What a lost answer leaves behind

The call either happened as a whole or did not happen. The table shows what the caller sees when it sends the same call again.

| Call | The resend after a lost answer |
|---|---|
| `enqueue`, `enqueueAll` | Answers `EXISTS` for a task that was stored. A task that was not stored is stored. |
| `allocateIds`, `allocateRunnerId` | Gives new ids. The lost ids are never used again, which leaves a gap and nothing else. |
| `queueId`, `typeId` | The id the name was given by the call that worked. A name has one id. |
| `claim` | A claim that worked has leased the tasks, and the caller never saw them. The resend finds nothing for them and the tasks run again after the lease expires. |
| `renew` | The same answer as a clean call. |
| `ack`, `ackAll` | `false` for a lease that was acknowledged already. |
| `fail`, `failExpired` | `LOST` for a lease that has ended already. |
| `reschedule`, `unclaim`, `finishCancelled`, `finishCancelledExpired` | `false` for a lease that has ended already. |
| `saveSteps`, `progress`, `heartbeat` | The same answer as a clean call. |
| `cancel` | `STALE` for a task that is not queued any more, and the same answer as a clean call for a leased task. |
| `delete` | `NOT_FOUND` for a task that was deleted. |
| `retry` of a dead task | `STALE`. |
| `pauseQueue`, `resumeQueue` | The same answer as a clean call. |
| `promote`, `reclaim`, `expire`, `purge`, `purgeRunners`, `releaseKeys` | The same effect. A resend of `releaseKeys` answers the count of keys it released itself, which can be 0 after a call that released them. |
| `putSchedule` | `UNCHANGED` for a definition that was stored. |
| `commitSchedule` | The fire is committed once. A resend answers `NOT_COMMITTED`, because the definition has moved on to its next fire. |
| `setNextFire` | `false` for a fire that was set already. |
| `deleteSchedule` | `NOT_FOUND` for a definition that was deleted. |
| `pauseSchedule` | The same answer as a clean call. |
| `resumeSchedule`, `triggerSchedule` | `STALE`, because the call that worked moved the revision on. A resume that the store did not apply, because the occurrence was still running, answers the same again. |

A call that the contract finds to leave a partial state is a bug, unless the interface says that it is only atomic per task. On Redis `enqueueAll`, `renew` and `ackAll` send a chunk of tasks per script, so a cut between two chunks leaves the earlier chunks done. This is the documented behaviour of the batch calls, and each task is still either done or not.

## Running the tests

| Store | Command |
|---|---|
| In-memory | `./mvnw -o verify -pl bth4j-testing -am -DskipITs -Dtest='InMemoryFaultContractTest,InMemoryRunnerFaultTest' -Dsurefire.failIfNoSpecifiedTests=false` |
| SQLite | `./mvnw -o verify -pl bth4j-sqlite -am -DskipITs -Dtest='SqliteFaultContractTest,SqliteRunnerFaultTest' -Dsurefire.failIfNoSpecifiedTests=false` |
| PostgreSQL | `./mvnw -o verify -pl bth4j-postgres -am -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false -Dit.test='PostgresFaultContractIT,PostgresRunnerFaultIT' -Dfailsafe.failIfNoSpecifiedTests=false` |
| Redis | the same with `-pl bth4j-redis` and `RedisFaultContractIT,RedisRunnerFaultIT`; add `-Pvalkey` for Valkey |

The PostgreSQL and Redis classes need Docker. The runner tests use real time with a lease of one second. `-Dbth4j.fault.seed=<number>` replaces the seeds of the two random runs, to repeat a failing run or to try others. The seed is printed when a run fails.

## Running them against another store

Subclass `FaultContract` and `RunnerFaultContract` from `bth4j-store-tests`, next to your subclasses of `StoreContract` and `RunnerContract`, and supply:

| Method | What you supply |
|---|---|
| `newFaulty(namespace)` | A `Faulty`: the store whose calls the plan cuts, the same namespace through a store that never fails, and the `FaultPlan` that counts and cuts. |
| `clock()` and `advance(duration)` | As for `StoreContract`, when the store takes a clock. |

The plan counts a fault point each time the store's driver does a round trip, and cuts the one that the test chooses. `FaultyDataSource` does this for a JDBC `DataSource`, and `FaultyStore` for a store that has no driver to wrap. Look at `SqliteFaults` and `RedisFaults` for the two ways to build it. Put the cutting layer below any connection pool, so that a cut connection is evicted as a real one would be.
