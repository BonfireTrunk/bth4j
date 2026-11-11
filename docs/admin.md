# Admin API and dashboard

`AdminOps` is the operator's view of a store and the set of operations an operator can run on it. `bth4j-admin-http` serves it as a JSON API, and the dashboard is a web app on top of that API. For health, issues and metrics, see [Monitoring](monitoring.md).

## On this page

- [Admin API](#admin-api)
- [Retrying a task](#retrying-a-task)
- [Unique keys after a retry](#unique-keys-after-a-retry)
- [Admin HTTP server](#admin-http-server)
- [Server settings](#server-settings)
- [Roles](#roles)
- [Signing in and sessions](#signing-in-and-sessions)
- [Failed logins](#failed-logins)
- [Endpoints](#endpoints)
- [Errors](#errors)
- [Transport and cross-origin calls](#transport-and-cross-origin-calls)
- [The dashboard](#the-dashboard)

## Admin API

`AdminOps` is the operator's view and set of operations over one store. `runner.admin()` also reports that runner's health and issues. `client.admin()` works in a process without a runner, where `health()` is empty and `issues()` is an empty list.

```java
AdminOps admin = runner.admin();

admin.counts();                                    // Map<queue name, StateCounts>
admin.queues();                                    // id, name, paused
admin.tasks(TaskState.DEAD, "mail", cursor, 100);  // oldest state change first, QUEUED and SCHEDULED earliest due; TaskViewPage(items, next); cursor null starts, next null ends; queue null for all
admin.task(id);                                    // Optional<TaskView>
admin.taskByKey("welcome-42");                     // the task holding that unique key, Optional<TaskView>
admin.retry(id, revision);                         // DEAD, CANCELLED or EXPIRED back to QUEUED, attempts reset
admin.cancel(id, revision, "reason");              // CANCELLED, REQUESTED or STALE
admin.cancelByKey("welcome-42", "reason");         // the holder of the key: CANCELLED, REQUESTED, STALE (changed meanwhile), NOT_FOUND (also a removed holder) or FINISHED
admin.delete(id, revision);                        // any state but LEASED
admin.pauseQueue("mail", true);
admin.schedules();                                 // ScheduleView: cron, zone id, misfire, fixed delay, paused, next fire
admin.pauseSchedule("poll-feed", false);           // resume a paused one; a fixed-delay definition is re-armed one delay from now
admin.triggerSchedule("nightly-report");           // fires at the next recurring check
admin.deleteSchedule("nightly-report");
admin.workers();
admin.unservedWork();
admin.purgeStaleWorkers(Duration.ofMinutes(10));
```

Every change to a task takes the revision the operator saw. If the task changed meanwhile, nothing is done and the call returns `false` or `STALE`.

| Call | Possible results |
|---|---|
| `retry(id, revision)` | `RetryOutcome.RETRIED` on success. `RetryOutcome.STALE` for a missing task or a revision or state mismatch. `RetryOutcome.DUPLICATE` when another task holds the retained key. |
| `cancel(id, revision, reason)` | `CANCELLED`, `REQUESTED` or `STALE`. See [Cancelling tasks](retries-and-cancelling.md#cancelling-tasks). |
| `cancelByKey(key, reason)` | `CANCELLED`, `REQUESTED`, `STALE` (the task changed meanwhile), `NOT_FOUND` (also when the holder was removed) or `FINISHED`. |

## Retrying a task

`AdminOps.retry` sends a DEAD, CANCELLED or EXPIRED task back to QUEUED.

```mermaid
flowchart TD
    A["admin.retry(id, revision)"] --> B{"Task exists, revision matches, state is DEAD, CANCELLED or EXPIRED?"}
    B -->|"no"| C["STALE"]
    B -->|"yes"| D{"Does another task hold the key this task retains?"}
    D -->|"yes"| E["DUPLICATE, nothing changes"]
    D -->|"no"| F["RETRIED: back to QUEUED, attempts reset"]
```

A retried task:

- keeps its failures and steps,
- resets its attempts to zero,
- loses its `startBy`, so a retried expired task does not expire again.

Inside a `keepFor` window the task retains its unique key without expiry while it is open.

- `taskByKey` and `cancelByKey` still reach it.
- A same-key enqueue answers `Duplicate` with its id.
- The `keepFor` setting is kept, so a fresh keep window starts when the retried task finishes.
- If another task holds the retained key, retry returns `RetryOutcome.DUPLICATE` and changes nothing. The admin HTTP route answers 409 with `{"code":"KEY_HELD","error":"the unique key is held by another task"}`. The dashboard matches the code.

## Unique keys after a retry

A key is released at some point. A retry after that release runs the task without a key.

| Key | Released when | A retry after the release |
|---|---|---|
| Default until-done key | The task finishes. | Runs the task **without a key**. |
| `keepFor` key | Its window passes. | Runs the task **without a key**. |

After the release, one of two things is true:

- If the key is free, an enqueue with that key is accepted and starts a second task.
- If another task has taken it, that task remains the holder.

## Admin HTTP server

`bth4j-admin-http` serves `AdminOps` as a JSON API under `/api/v1` on the JDK HTTP server. A browser (the dashboard) or any API client talks to the server. The server calls `AdminOps`, which works on the store.

```mermaid
flowchart LR
    B["Browser (dashboard)"] -->|"session cookie, plus X-CSRF-Token on POST and DELETE"| S["AdminHttpServer, /api/v1"]
    C["API client"] -->|"session cookie, plus X-CSRF-Token on POST and DELETE"| S
    S --> A["AdminOps"]
    A -->|"reads and changes"| T["Store"]
    A -.->|"health and issues of one runner"| R["BackgroundRunner"]
    R --> T
```

```java
var server = AdminHttpServer.builder()
        .admin(runner.admin())
        .bind("127.0.0.1", 8787)                          // the default
        .user("ops", System.getenv("BTH4J_OPS_PASSWORD"), Role.ADMIN)
        .user("support", System.getenv("BTH4J_SUPPORT_PASSWORD"), Role.VIEWER)
        .staticDir(Path.of("dashboard"))                  // serves the dashboard at http://127.0.0.1:8787/
        .build();
server.start();
// on shutdown: server.stop();
```

At least one user is required. A server runs once: `start()` after `stop()` throws, so build a new one.

### Server settings

| Setting | What it does |
|---|---|
| `admin(AdminOps)` | The `AdminOps` the server exposes. |
| `bind(host, port)` | Where the server listens. The default is `127.0.0.1` and `8787`. |
| `user(name, password, role)` | Adds a user with a [role](#roles). At least one user is required. |
| `staticDir(path)` | Serves the dashboard from that directory at the server's address. |
| `sessionTtl` | How long a session lasts after the last request. The default is 12 hours. |
| `https(SSLContext)` | Makes the server speak HTTPS and marks the cookie `Secure`. |
| `secureCookie(true)` | Marks the cookie `Secure` when the server itself speaks plain HTTP. |
| `trustedProxies(addresses)` | Names the reverse proxies in front of the server, so that failed logins are counted per client and not per proxy. See [Failed logins](#failed-logins). |
| `corsOrigin(origin)` | Lets a browser app on another origin call the API with credentials. |

### Roles

| Role | Can read | Can change |
|---|---|---|
| `Role.ADMIN` | Yes | Yes |
| `Role.VIEWER` | Everything, including task payloads | No. Every mutation answers 403. |

### Signing in and sessions

A client signs in with `POST /api/v1/session` and `{"user", "password"}`. The reply sets the `bth4j_session` cookie and carries a CSRF token. The client sends the token as `X-CSRF-Token` on every POST and DELETE. `GET /api/v1/me` returns the token again, so a second browser tab that shares the cookie can ask for it.

```mermaid
sequenceDiagram
    participant C as Client
    participant S as AdminHttpServer
    C->>S: POST /api/v1/session with user and password
    S-->>C: bth4j_session cookie and a CSRF token in the reply
    C->>S: GET requests with the cookie
    C->>S: POST and DELETE requests with the cookie and X-CSRF-Token
    S-->>C: Cookie Max-Age renewed on every authenticated response
```

| What | How it works |
|---|---|
| Cookie | `bth4j_session`, `HttpOnly`, `SameSite=Strict`. |
| Storage | Sessions are kept in memory and end when the server stops. |
| Lifetime | A session lasts 12 hours after the last request (`sessionTtl`). |
| Cookie expiry | Every authenticated response renews the cookie's `Max-Age`, so the browser's expiry follows the sliding session expiry. |
| Sessions per user | At most 20. A login past that ends that user's oldest session. |
| Failed logins | Counted by address and by user name. The fifth failure blocks for 10 seconds (HTTP 429). See [Failed logins](#failed-logins). |

### Failed logins

The server counts a failed login twice, and either count can block the next login:

| Counted by | What the fifth failure blocks | Notes |
|---|---|---|
| Where the login comes from | Logins from there, for 10 seconds | An IPv4 address counts by itself. An IPv6 address counts by its /64 network, because one client usually owns the whole network. |
| The user name it gives | Logins with that name from anywhere, for 10 seconds | Every name is counted, also one that does not exist, so a block does not show which users exist. |

These rules apply to both counts:

- A blocked login is answered 429 with `Retry-After`. It is not counted and its password is not looked at.
- A successful login takes back only its own attempt. It does not clear the failures counted before it.
- Failures are forgotten when no attempt followed them for 10 minutes.
- Someone who knows a user name can keep that user out by sending five wrong passwords every 10 seconds. Keep the server off networks where that is a concern.

#### Behind a reverse proxy

The server sees the proxy's address on every request. Name the proxy with `trustedProxies`, and the server counts by the client address the proxy reports in `X-Forwarded-For`:

```java
AdminHttpServer.builder()
        .trustedProxies("10.0.0.5")            // one address, or a range such as "10.0.0.0/8"
        .secureCookie(true)
        // ...
```

```mermaid
flowchart TD
    R["Login request"] --> P{"Does it come from a trusted proxy?"}
    P -->|No| A["Count by the address it comes from. X-Forwarded-For is ignored."]
    P -->|Yes| F["Read X-Forwarded-For from the right"]
    F --> L{"Is this entry a trusted proxy?"}
    L -->|Yes| N["Go one entry to the left"]
    N --> L
    L -->|No| C["Count by this address"]
```

| Point | What to know |
|---|---|
| Without `trustedProxies` | The header is ignored and all clients behind the proxy count as one address: five failures from anyone block every login for 10 seconds. The server logs one warning when a login carries the header from an address that is not trusted. |
| What the proxy must do | Set `X-Forwarded-For` itself, or append the address it saw to the header it received. A proxy that passes a client's header on unchanged lets the client choose the address it is counted by. |
| What to name | Only proxies. A request from a trusted address can state any client address, so a range must not include clients. |
| An entry that is not an IP address | It ends the search, and the login counts by the proxy that reported it. An entry may carry a port. |
| Other headers | Only `X-Forwarded-For` is read, not `Forwarded`. |

### Endpoints

Task ids and revisions are JSON strings.

| Endpoint | What it offers |
|---|---|
| `me` | The signed-in user, the role and the session's CSRF token. |
| `session` | Sign-in. |
| `health` | The runner's health. |
| `overview` | The overview. |
| `queues` | The queues, with `pause` and `resume`. |
| `tasks` | A list with `state`, `queue`, `cursor` and `limit` (up to 200). One task, its `payload`, `retry`, `cancel` and delete. |
| `tasks/by-key?key=` | Reads the task that holds a unique key. |
| `tasks/by-key/cancel?key=` | Cancels the task that holds the key, with a body of `{"reason"}` and no revision. |
| `schedules` | The schedules, with `pause`, `resume`, `trigger` and delete. |
| `workers` | The workers. |
| `unserved` | Queued work that no live accepting worker can process. |
| `issues` | The runner's issues. |

Viewers may read, and admins may also change (see [Roles](#roles)).

#### Listing tasks

`GET /api/v1/tasks` answers `{"items": [...], "next": "<cursor>" | null}`.

- Pass `next` as `cursor` for the following page. `next` is null after the last page.
- A missing or empty `cursor` starts at the beginning.
- A malformed `cursor` is answered 400.

#### Tasks by key

- Viewers may read a task by key. Admins may cancel it.
- A blank key or one over 256 characters is answered 400.
- A read answers 404 when no task holds the key.
- A cancel answers 404 when no task holds the key, and 409 when the holder is already finished or changed meanwhile.

#### Workers and unserved work

- `GET /api/v1/workers` includes `queues` and `types` as name lists and the boolean `accepting`.
- `GET /api/v1/unserved` returns a list of `{"queue", "type", "waiting"}`, using the one-minute heartbeat window. A null type means the whole queue is unserved and `waiting` is its queued count. Otherwise `waiting` is the exact queued count of that type. For what counts as unserved, see [Unserved work](monitoring.md#unserved-work).
- Both endpoints are available to viewers.

### Errors

| Status | When |
|---|---|
| 400 | A malformed `cursor`. An unknown queue name in the tasks filter. A missing or blank by-key key, or one over 256 characters. |
| 403 | A viewer sent a mutation. |
| 404 | An unknown id. An unknown queue name on pause and resume. No task holds the key on a by-key read or cancel. |
| 409 | A stale revision. A by-key cancel when the holder is already finished or changed meanwhile. A retry blocked by a held key, with `{"code":"KEY_HELD", ...}`. |
| 422 | A task whose stored value cannot be read, with `code: "UNREADABLE_TASK"`. |
| 429 | Too many failed logins from one address or for one user name. |

### Transport and cross-origin calls

| Case | What to do |
|---|---|
| Default | The server speaks plain HTTP unless `https(SSLContext)` is set. Keep it on the loopback interface or behind TLS. |
| TLS in the server | Set `https(SSLContext)`. It also marks the cookie `Secure`. |
| A TLS-terminating proxy in front | The server itself speaks plain HTTP, so set `secureCookie(true)` to mark the cookie `Secure`. Also set `trustedProxies`, so that [failed logins](#failed-logins) are counted per client. |
| A browser app on another origin calls the API | Set `corsOrigin("https://dashboard.example.com")`. It lets that app call the API with credentials. |

## The dashboard

The dashboard is the static Vue 3 app in `dashboard/`. It has no build step. It ships its pinned Vue runtime in `dashboard/vendor/` and loads nothing from the network.

| Page | What it shows |
|---|---|
| Overview | The overview, and unserved work. |
| Queues | The queues. |
| Tasks | Tasks with filters, and a detail view: timeline, steps, failures and the payload. |
| Dead tasks | The dead tasks. |
| Schedules | The schedules. |
| Workers | The workers, with coverage and quiet status. |
| Issues | The issues. |

Admins can also retry, cancel (with a reason), delete, pause and trigger.

The files come from the repository checkout. You can serve them in two ways:

| Way | How |
|---|---|
| From the admin server | Point `staticDir` at the `dashboard` directory. |
| From your own static host | Copy the directory to your static host and set `window.BTH4J_API` in `index.html`. The dashboard and API hosts must then share a registrable domain, because of the `SameSite=Strict` cookie. The admin server must also allow the dashboard's origin, for example `.corsOrigin("https://dashboard.example.com")` (see [Transport and cross-origin calls](#transport-and-cross-origin-calls)). |

Details are in [dashboard/README.md](../dashboard/README.md). `dashboard/dev/fake-api.py` serves sample data without Java.
