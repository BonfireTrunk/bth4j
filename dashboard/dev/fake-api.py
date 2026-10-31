#!/usr/bin/env python3
"""Fake bth4j admin API plus static file server for the dashboard (standard library only).

    python3 dashboard/dev/fake-api.py [port]      # default 8787, then open http://127.0.0.1:8787/

Users: admin / admin (ADMIN), viewer / viewer (VIEWER). Data lives in memory and resets on restart.
"""
import json
import os
import secrets
import sys
from datetime import datetime, timedelta, timezone
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
USERS = {"admin": ("admin", "ADMIN"), "viewer": ("viewer", "VIEWER")}
STATES = ["QUEUED", "SCHEDULED", "LEASED", "SUCCEEDED", "DEAD", "CANCELLED", "EXPIRED"]
BASE_ID = 9007199254740993  # above 2**53 on purpose: ids must stay strings in the UI

NOW = datetime.now(timezone.utc)


def iso(delta_s):
    return (NOW + timedelta(seconds=delta_s)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


queues = {"default": {"id": 1, "name": "default", "paused": False}, "mail": {"id": 2, "name": "mail", "paused": False},
          "reports": {"id": 3, "name": "reports", "paused": True}}
tasks = {}
sessions = {}


def make_tasks():
    qnames = list(queues)
    for i in range(130):
        state = STATES[(i * 3 + i // 7) % len(STATES)]
        tid = str(BASE_ID + i)
        t = {
            "id": tid, "revision": str(1 + i % 5), "queue": qnames[i % 3], "event": ["send-mail", "build-report", "sync-account"][i % 3],
            "payloadVersion": 1, "state": state, "attempts": 1 if state != "QUEUED" else 0, "maxAttempts": 5,
            "created": iso(-86400 + i * 600), "due": iso(-86000 + i * 600), "stateChanged": iso(-3600 + i * 20),
            "workerId": 7 if state == "LEASED" else None, "leaseUntil": iso(30) if state == "LEASED" else None,
            "claimedAt": iso(-40) if state in ("LEASED", "SUCCEEDED", "DEAD") else None,
            "account": "acme" if i % 2 else None, "headers": {"trace": "t-%d" % i} if i % 4 == 0 else {},
            "uniqueKey": "uk-%d" % i if i % 5 == 0 else None, "scheduleId": "nightly-report" if i % 11 == 0 else None,
            "retryOf": None, "startBy": None, "maxRun": "PT15M" if i % 3 == 0 else None,
            "cancelRequestedAt": iso(-100) if state == "CANCELLED" else None,
            "cancelReason": "no longer needed" if state == "CANCELLED" else None,
            "progress": "step 2 of 3" if state == "LEASED" else None,
            "steps": {"fetch": {"at": iso(-3000), "result": "ok"}, "render": {"at": iso(-2900), "result": None}} if state in ("SUCCEEDED", "DEAD") else {},
            "failures": [], "lastFailure": None,
        }
        if state == "DEAD":
            t["attempts"] = 5
            t["failures"] = [{"attempt": n, "at": iso(-3000 + n * 60), "type": "java.io.IOException", "message": "connection reset %d" % n,
                              "stack": "java.io.IOException: connection reset %d\n\tat app.Mailer.send(Mailer.java:42)\n\tat app.Worker.run(Worker.java:17)" % n} for n in range(1, 6)]
            t["lastFailure"] = t["failures"][-1]
        tasks[tid] = t


def counts():
    out = {q: {s: 0 for s in STATES} for q in queues}
    for t in tasks.values():
        out[t["queue"]][t["state"]] += 1
    return out


schedules = {
    "nightly-report": {"id": "nightly-report", "event": "build-report", "queue": "reports", "account": None, "cron": "0 0 2 * * *", "zone": "Europe/Berlin",
                       "misfire": "FIRE_ONCE", "fixedDelay": None, "overlapSkip": True, "paused": False, "nextFire": iso(7200), "maxAttempts": 3},
    "mail-pump": {"id": "mail-pump", "event": "send-mail", "queue": "mail", "account": None, "cron": None, "zone": None,
                  "misfire": "SKIP", "fixedDelay": "PT30S", "overlapSkip": False, "paused": True, "nextFire": iso(25), "maxAttempts": 5},
}
workers = [
    {"id": 7, "label": "host-a/1234", "startedAt": iso(-7200), "lastSeen": iso(-2), "running": 3},
    {"id": 8, "label": "host-b/9876", "startedAt": iso(-9000), "lastSeen": iso(-300), "running": 0},
]
issues = [
    {"at": iso(-600), "kind": "unknown-event", "detail": "No handler for event 'legacy-export'", "taskId": str(BASE_ID + 3)},
    {"at": iso(-60), "kind": "store-error", "detail": "Connection refused (retrying)", "taskId": None},
    {"at": iso(-5000), "kind": "lease-lost", "detail": "Lease expired while running", "taskId": str(BASE_ID + 9)},
]
health = {"status": "RUNNING", "workerId": 7, "running": 3, "lastClaimAt": iso(-2), "lastMaintenanceAt": iso(-15),
          "lastStoreErrorAt": iso(-60), "lastStoreError": "Connection refused", "issueCount": len(issues)}


class Handler(BaseHTTPRequestHandler):
    server_version = "fake-bth4j"

    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (self.command, self.path))

    def cors(self):
        origin = self.headers.get("Origin")
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-CSRF-Token")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
            self.send_header("Vary", "Origin")

    def send_json(self, status, body, extra=None):
        data = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        self.cors()
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        if body is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def session(self):
        c = SimpleCookie(self.headers.get("Cookie", ""))
        m = c.get("bth4j_session")
        return sessions.get(m.value) if m else None

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n))
        except ValueError:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self.cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_DELETE(self):
        self.route("DELETE")

    def static(self, path):
        rel = path.lstrip("/") or "index.html"
        full = os.path.abspath(os.path.join(ROOT, rel))
        if not full.startswith(ROOT + os.sep) or not os.path.isfile(full):
            full = os.path.join(ROOT, "index.html")
        types = {".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".md": "text/plain"}
        with open(full, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", types.get(os.path.splitext(full)[1], "application/octet-stream") + "; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def route(self, method):
        url = urlparse(self.path)
        path = url.path
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if not path.startswith("/api/"):
            return self.static(path) if method == "GET" else self.send_json(404, {"error": "not found"})
        p = path[len("/api/v1"):] if path.startswith("/api/v1") else path
        if p == "/session" and method == "POST":
            b = self.body()
            u = USERS.get(b.get("user"))
            if not u or u[0] != b.get("password"):
                return self.send_json(401, {"error": "Wrong user or password"})
            sid, csrf = secrets.token_hex(16), secrets.token_hex(16)
            sessions[sid] = {"user": b["user"], "role": u[1], "csrf": csrf}
            return self.send_json(200, {"csrf": csrf, "user": b["user"], "role": u[1]},
                                  {"Set-Cookie": "bth4j_session=%s; HttpOnly; SameSite=Strict; Path=/" % sid})
        s = self.session()
        if not s:
            return self.send_json(401, {"error": "not signed in"})
        if p == "/session" and method == "DELETE":
            sessions.pop(next((k for k, v in sessions.items() if v is s), None), None)
            return self.send_json(204, None)
        if method != "GET":
            if self.headers.get("X-CSRF-Token") != s["csrf"]:
                return self.send_json(403, {"error": "missing or wrong CSRF token"})
            if s["role"] != "ADMIN":
                return self.send_json(403, {"error": "viewers cannot change anything"})
        segs = [unquote(x) for x in p.strip("/").split("/")]
        try:
            self.dispatch(method, segs, q)
        except KeyError:
            self.send_json(404, {"error": "unknown id"})

    def dispatch(self, method, segs, q):
        head = segs[0]
        if head == "me":
            return self.send_json(200, {"user": self.session()["user"], "role": self.session()["role"]})
        if head == "health":
            return self.send_json(200, health)
        if head == "overview":
            return self.send_json(200, {"counts": counts(), "workers": len(workers), "issues": len(issues), "health": health})
        if head == "queues":
            if len(segs) == 1:
                c = counts()
                return self.send_json(200, [dict(v, counts=c[k]) for k, v in queues.items()])
            queues[segs[1]]["paused"] = segs[2] == "pause"
            return self.send_json(204, None)
        if head == "workers":
            return self.send_json(200, workers)
        if head == "issues":
            return self.send_json(200, issues)
        if head == "schedules":
            if len(segs) == 1:
                return self.send_json(200, list(schedules.values()))
            sc = schedules[segs[1]]
            if method == "DELETE":
                del schedules[segs[1]]
            elif segs[2] == "pause":
                sc["paused"] = True
            elif segs[2] == "resume":
                sc["paused"] = False
            return self.send_json(204, None)
        if head == "tasks":
            return self.tasks_api(method, segs, q)
        self.send_json(404, {"error": "not found"})

    def tasks_api(self, method, segs, q):
        if len(segs) == 1:
            limit = min(int(q.get("limit", 50)), 200)
            after = int(q["after"]) if q.get("after") else -1
            rows = [t for t in sorted(tasks.values(), key=lambda t: int(t["id"])) if int(t["id"]) > after
                    and (not q.get("state") or t["state"] == q["state"]) and (not q.get("queue") or t["queue"] == q["queue"])]
            return self.send_json(200, rows[:limit])
        t = tasks[segs[1]]
        if len(segs) == 2 and method == "GET":
            return self.send_json(200, t)
        if len(segs) == 3 and segs[2] == "payload":
            return self.send_json(200, {"payload": json.dumps({"to": "ops@example.com", "template": "welcome", "n": int(t["id"]) % 100})})
        rev = (self.body().get("revision") if method == "POST" else q.get("revision"))
        if str(rev) != t["revision"]:
            return self.send_json(409, {"error": "stale revision"})
        if method == "DELETE":
            del tasks[t["id"]]
            return self.send_json(204, None)
        if segs[2] == "retry":
            t.update(state="QUEUED", attempts=0, revision=str(int(t["revision"]) + 1), stateChanged=iso(0))
        elif segs[2] == "cancel":
            t.update(state="CANCELLED", cancelReason=self.body().get("reason"), cancelRequestedAt=iso(0),
                     revision=str(int(t["revision"]) + 1), stateChanged=iso(0))
        return self.send_json(200, t)


if __name__ == "__main__":
    make_tasks()
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8787
    print("fake bth4j API + dashboard on http://127.0.0.1:%d/  (admin/admin, viewer/viewer)" % port)
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
