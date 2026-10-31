# bth4j dashboard

A static single-page operator dashboard for the bth4j admin HTTP API. No build step, no package manager:
`index.html`, `app.css`, `api.js`, `ui.js`, `app.js` and `pages/*.js` (plain ES modules).

Pages: overview, queues, tasks (filter, cursor paging, detail), dead tasks, schedules, workers, issues.

## Serving it

### Same origin (recommended): `staticDir`

Point the admin HTTP server at this directory and open the server's address. The session cookie is
`SameSite=Strict`, so same origin is the simplest setup and needs no CORS.

```java
AdminHttpServer.builder()
        .admin(adminOps)
        .user("ops", "change-me", Role.ADMIN)
        .user("support", "change-me-too", Role.VIEWER)
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

The admin server must then allow the dashboard's origin (`.corsOrigin("https://dashboard.example.com")`).
Requests use `credentials: 'include'`. Note the session cookie is `SameSite=Strict`, so the dashboard host
and the API host must be the same site (same registrable domain), otherwise the browser will not send it.

### Pinned Vue version and air-gapped deployments

`index.html` loads Vue from a CDN at an exact version:

```
https://cdn.jsdelivr.net/npm/vue@3.5.13/dist/vue.global.prod.js
```

For a deployment without internet access, download that file, put it next to `index.html`, and change the
one `<script src="...">` line to `vue.global.prod.js`. Nothing else loads from the network.

## Sign-in and roles

Sign in with a user configured on the admin server. The login response carries a CSRF token which the
dashboard keeps in memory and in `sessionStorage` (cleared when the tab closes) and sends as
`X-CSRF-Token` on every mutation. A `401` returns you to the login page.

- **Viewer**: can look at everything, including task payloads on request. Every action button
  (retry, cancel, delete, pause/resume, trigger, retry-all) is visible but disabled.
- **Admin**: all of the above. Every mutation asks for confirmation; cancel also requires a reason.
  Task actions send the revision shown on screen. If the task changed meanwhile the server answers `409`
  and the dashboard shows a "changed meanwhile, reload" prompt instead of overwriting.

Task ids and revisions are handled as strings and never parsed to numbers (they can exceed 2^53).

## Trying it without Java

`dev/fake-api.py` is a Python 3 standard-library server that implements the API shapes with sample data
(130 tasks across all states, 2 schedules, 2 workers, 3 issues) and also serves this directory:

```
python3 dashboard/dev/fake-api.py [port]     # default 8787
```

Open `http://127.0.0.1:8787/` and sign in as `admin` / `admin` or `viewer` / `viewer`. State lives in memory
and resets on restart. It is a development aid only; it does not model the real server's semantics beyond
revision checks (`409`), CSRF (`403`) and roles (`403`).
