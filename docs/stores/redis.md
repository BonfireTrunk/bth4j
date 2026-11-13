# Redis store

`RedisTaskStore` keeps your tasks in Redis or Valkey, so runners on several machines can share the work. This page shows how to set it up, what it needs, what it does, and where it differs from the other stores.

On this page:

- [Set up](#set-up)
- [Requirements](#requirements)
- [What the store does](#what-the-store-does)
- [Caveats](#caveats)
- [Inside the Redis store](#inside-the-redis-store)

## Set up

Create the store from a Jedis client and a namespace:

```java
var store = RedisTaskStore.builder().client(jedis).namespace("my-app").build();   // UnifiedJedis (for example a RedisClient) and a namespace
```

The namespace prefixes every key and carries the Redis Cluster hash tag, so all keys of a store share one slot. The store does not close the Jedis client.

## Requirements

| Item | Requirement |
|---|---|
| Module | `bth4j-redis`, which uses Jedis |
| Server | Redis 7.4 or newer, or Valkey 9 or newer. The store expires the key fields of finished tasks with hash field expiry, and `init` refuses a server that does not have it. |
| Tested on | standalone Redis 8.10 and Valkey 9.1.3 |
| `maxmemory-policy` | `noeviction`. `init` refuses to start under any other policy (see [Memory eviction](#memory-eviction)). |
| Schema version | 7, stored in the namespace's `meta` hash. `init` refuses a namespace that holds keys of the store without a schema version, and a namespace with another version, which includes every namespace written by bth4j 4.x or older; there is no migration. Start with a new namespace. |
| Redis Cluster | Not tested. The namespace's slot must not be resharded while runners use it (see [Redis Cluster](#redis-cluster)). |

## What the store does

| Operation | What the store does |
|---|---|
| Clock | Takes the current time from the Redis server (`TIME`). See [which clock decides](../stores.md#which-clock-decides). |
| Enqueue | Writes a random per-record token with the other fields of the task. A resend is judged by that token (see the table below). |
| Enqueue and acknowledgement batches | Run as one script per group of tasks. |
| Claim | Runs both grant passes in one atomic script. It merges the eligible types by `(due, lexical id)` and leaves foreign tasks unchanged. Its scan budget is `min(count * 4, 1000)` eligible entries per queue across both passes. Its cost grows with the types that have queued tasks, not with the types registered. A claim leaves a task whose `startBy` has passed in its queue for maintenance to expire. |
| Queue listings | Merge the active type zsets by `(due, id as text)`, the order a claim takes them in, and retain their position cursors when a type empties. A page of up to 500 tasks takes two round trips whatever the number of queues and types, one script call that picks the ids and one pipeline that reads the tasks, and the script blocks the server for as long as its sets and its page need, not for the number of tasks. A page finds its place by rank, so many tasks due in one millisecond cost a page no more than any others. |
| Reclaim | One script reads the server's time, the expired leases and the fields maintenance needs of each, so a pass is one round trip whether it finds nothing or a full page of leases. |
| `queuedByType()` and the counts | Read exact zset cardinalities of the active types in one script, without task hash reads. |
| Finished tasks | Are listed and purged from one zset per state and queue. |
| `upcomingSchedules(limit)` | One `ZRANGE` of the `cron` zset (its score is the next fire time, and equal scores are in byte order of the id, the order of `ScheduleStore.ID_ORDER`), then the hashes of those schedules in one pipeline. |
| `schedule(id)` | One `HGETALL`. |
| `schedules(afterId, limit)` | One `ZRANGE crondefs <min> + BYLEX LIMIT 0 limit` (`min` is `-`, or `(` and the id after which the page starts), then the hashes of that page in one pipeline: 101 commands in 2 round trips for a page of 100, whatever the number of schedules. `nextFireTimes()` reads all ids with one `ZRANGE 0 -1` and every hash, as many commands as there are schedules plus one. |
| Removing a task whose unique key window is open | Leaves only a key string, which expires at the end of the window. |
| Runner registration | Sets `accepting` to 1. |
| Runner heartbeat | One script atomically updates the runner's time, running count and accepting flag. It also writes the hash of a removed runner again, with its label, queues and types. |

How the store answers a resent enqueue:

| Resend | Answer |
|---|---|
| A transport resend with the same token | stored again |
| A different token under the same id | exists, before the unique key is checked |

The token only helps while the task exists. With a zero succeeded retention, a task that already ran and was removed is stored again by a resend.

## Caveats

Caveats that apply to every store, such as what a crash can lose, time precision and the order of tasks due at the same time, are on the [Stores](../stores.md) page. Enqueueing in your own transaction needs an outbox with Redis: see [Enqueue inside your own transaction](../stores.md#enqueue-inside-your-own-transaction).

### Redis Cluster

Redis Cluster is not tested. The scripts declare only one key and derive the others, so the namespace's slot must not be resharded while runners use it. During a slot migration a script can fail after part of its writes.

### Memory eviction

`init` reads `maxmemory_policy` from `INFO memory` and refuses to start with a `TaskConfigurationException` unless it is `noeviction`. Under any other policy Redis removes keys when it reaches `maxmemory`:

| Policy | What Redis removes | Effect on bth4j |
|---|---|---|
| `noeviction` | nothing | none; `init` accepts it |
| `volatile-*` | the keys with an expiry | among them the keys that hold the unique-key window of a removed task, so a duplicate is accepted |
| `allkeys-*` | any key | task data as well |

The defaults differ by product:

| Product | Default policy |
|---|---|
| Redis Cloud databases and most Redis Software databases | `volatile-lru` |
| Self-hosted Redis or Valkey | `noeviction` |

When the connection may not run `INFO` (an ACL without `+info`, a renamed command) or the reply has no `maxmemory_policy`, `init` logs a warning to `Loggers.REDIS_STORE` and starts without the check. PostgreSQL and SQLite never remove data on their own.

### Claims behind tasks past `startBy`

A Redis claim looks at no more than `min(count * 4, 1000)` eligible entries per queue across both passes. Here `count` is the number of tasks it asks for: the free slots, at most `claimBatchSize`. The claim leaves a task whose `startBy` has passed in the queue for maintenance to expire.

When more tasks past their `startBy` than that budget sit at the front of a queue, the queue claims nothing until the next maintenance pass expires them. That is at most `maintenanceCheckInterval` later (5 s by default). See [Configuration](../configuration.md) for both settings.

PostgreSQL, SQLite and the in-memory store skip those tasks in the claim.

## Inside the Redis store

Schema version 7 uses these keys:

| Key | Type | What it holds |
|---|---|---|
| `meta` | hash | the schema version, in the field `schema` |
| `seq:id`, `seq:worker`, `seq:queue`, `seq:type`, `cronrev` | counters | the last task id, runner id, queue id, type id and schedule revision handed out |
| `reg:queue`, `reg:type` | hash | queue and type names with their ids |
| `paused` | set | the ids of the paused queues |
| `t:<id>` | hash | a task, with only the fields that are set |
| `d:<id>` | string | the payload of a task |
| `st:<id>` | string | the steps of a task, as JSON |
| `f:<id>` | list | the failures of a task, the last 20 |
| `q:<queue-id>:<type-id>` | zset | queued tasks of one type in one queue, scored by due milliseconds |
| `active:<queue-id>` | set | the types whose zset in that queue is not empty |
| `sched:<queue-id>` | zset | scheduled tasks of one queue, scored by due milliseconds |
| `leased` | zset | leased tasks, scored by the end of the lease |
| `startby` | zset | queued and scheduled tasks that have a start-by time, scored by it |
| `ts:<digest>` | set | a runner's known types, named by a digest of the sorted type ids and shared by runners with the same types |
| `idx:<state>:<queue-id>` | zset | the leased and the finished tasks of one state in one queue, scored by the time of the state change; listed and purged from here |
| `k:<key>` | string | the id of the task that holds a unique key. With a keep window it expires at the end of the window, also when the task was removed sooner. |
| `w:<runner-id>` | hash | a runner: label, start time, last heartbeat, running count, accepting flag, queues and types |
| `workers` | zset | runner ids, scored by the last heartbeat |
| `cron:<schedule-id>` | hash | a schedule |
| `cron` | zset | the next fire time of every schedule that is not paused |
| `crondefs` | zset | the ids of all schedules, every score 0, so it is ordered by the bytes of the id (`ScheduleStore.ID_ORDER`); the schedule listing pages through it |

Every key starts with `{<namespace>}:`. The keys of runners kept the word "worker" from earlier releases.

Other details:

| Item | Detail |
|---|---|
| Task hash field `wt` | the random per-record write token that enqueue stores with the other fields |
| Task and recurring-definition hashes, field `ty` | the type id |
| Recurring-definition hash, field `trg` | set to 1 while the next fire time is one an operator triggered |
| Runner hash, field `accepting` | set to 1 on registration; the heartbeat script updates it |
| `ts:<digest>` lifetime | a claim that finds less than half a day left on it extends it to a day, so a runner that polls an idle queue writes nothing; it expires between twelve and twenty-four hours after the last claim that named it, and a claim writes it again when it is missing |
| Work per call | lease renewals and batched acknowledgements go in groups of 256 leases, and one call removes at most 500 stale runners, so that no call blocks the server for long |
| Zset writes | are collected per key and sent as one command per key at the end of a script |
| Task ids in scripts | travel as text, because a Lua number is a double and keeps 53 bits; revisions, attempts and times are numbers |
| Time | every script reads `TIME` itself, so due times, leases and retention follow the Redis server; reading the clock before a write needs effect replication, the default since Redis 7.0 |
| Claim | takes the intersection of the `active:<queue-id>` set and the `ts:<digest>` set, so its cost grows with the types that have queued tasks, not with the types registered |

### Memory encodings

| Structure | Setting | Compact while | Otherwise |
|---|---|---|---|
| Task hash | `hash-max-listpack-value` (default 64) | every value is at most that many bytes (a listpack) | a hashtable, about 5 times larger for that task |
| Zsets (queues, `leased`, the state indexes) | `zset-max-listpack-entries` (default 128) | there are at most that many entries (a listpack) | a skiplist, which costs about 6 times more per entry |

A header map, account, unique key, progress text or cancel reason longer than that setting (64 bytes by default) makes the task hash a hashtable. Raise `hash-max-listpack-value` if your tasks carry such values.

Raising `zset-max-listpack-entries` trades CPU for memory.
