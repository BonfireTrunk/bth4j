# bth4j dashboard

A static single-page operator dashboard for the bth4j admin HTTP API. No build step, no package manager:
`index.html`, `app.css`, `api.js`, `ui.js`, `app.js` and `pages/*.js` (plain ES modules).

Pages: overview, queues, tasks (filter, cursor paging, detail), dead tasks, schedules, runners, issues.
Runners shows each runner's queues, registered types and accepting state (`accepting` from the API).
A live runner that is not accepting is shown as quiet; a stale heartbeat still shows the silence warning.
Overview polls `GET /api/v1/unserved` for queued work without a live accepting runner (heartbeat within
one minute). Whole-queue and per-type counts are exact at the queued-count read. Quiet runners do not
contribute coverage, and paused queues are skipped.

## Serving it

### Same origin (recommended): `staticDir`

Point the admin HTTP server at this directory and open the server's address. The session cookie is
`SameSite=Strict`, so same origin is the simplest setup and needs no CORS.

```java
AdminHttpServer.builder()
        .admin(adminOps)
        .addUser("ops", "change-me", Role.ADMIN)
        .addUser("support", "change-me-too", Role.VIEWER)
        .staticDir(Path.of("dashboard"))
        .build()
        .start();
```

Then browse to `http://127.0.0.1:8787/`.

### Any static host

Serve the directory from any static host and tell the page where the API lives, in `index.html`:

```html
<script>window.BTH4J_API = "https://host:8787";</script>
```

The admin server must then allow the dashboard's origin (`.addCorsOrigin("https://dashboard.example.com")`).
Requests use `credentials: 'include'`. Note the session cookie is `SameSite=Strict`, so the dashboard host
and the API host must be the same site (same registrable domain), otherwise the browser will not send it.

### Vue runtime

`index.html` loads Vue from `vendor/vue.global.prod.js`, a copy of Vue 3.5.13 kept in this repository (source, hashes and
update steps in `vendor/README.md`). Nothing loads from the network, so the dashboard works without internet access.
The server's Content-Security-Policy therefore allows scripts from the server itself only, plus `'unsafe-eval'`: the Vue
global build compiles the `template:` strings of the pages in the browser with `new Function`, which needs it.

## Sign-in and roles

Sign in with a user configured on the admin server. The login response carries a CSRF token which the
dashboard keeps in memory and in `sessionStorage` (cleared when the tab closes) and sends as
`X-CSRF-Token` on every mutation. When a page loads, the dashboard asks `GET /me` for the session's
token, so a new tab uses the session of the other tabs without a second sign-in. A `401` returns you to
the login page.

- **Viewer**: can look at everything, including task payloads. Every action button
  (retry, cancel, delete, pause/resume, trigger, retry-all) is visible but disabled.
- **Admin**: all of the above. Every mutation asks for confirmation; cancel also requires a reason.
  Task actions send the revision shown on screen. If the task changed meanwhile the server answers `409`
  with the code `STALE` and the dashboard shows a "changed meanwhile, reload" prompt instead of overwriting.
  A task whose state refuses the action (`WRONG_STATE`) shows the server's message, and a task that is gone
  (`NOT_FOUND`) is reported as no longer existing.
  Trigger moves a schedule's next fire time to now, and the runner creates the task on its next scheduling
  round; it is disabled for a paused schedule, which does not fire (resume it first).

Task ids and revisions are handled as strings and never parsed to numbers (they can exceed 2^53).

## Trying it without Java

`dev/fake-api.py` is a Python 3 standard-library server that implements the API shapes with sample data
(130 tasks across all states, 2 schedules, 2 runners, 3 issues) and also serves this directory:

```
python3 dashboard/dev/fake-api.py [port]     # default 8787
```

Open `http://127.0.0.1:8787/` and sign in as `admin` / `admin` or `viewer` / `viewer`. State lives in memory
and resets on restart. It is a development aid only; it does not model the real server's semantics beyond
revision checks (`409`), CSRF (`403`) and roles (`403`).
