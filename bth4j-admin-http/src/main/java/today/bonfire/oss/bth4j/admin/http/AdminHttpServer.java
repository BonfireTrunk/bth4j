package today.bonfire.oss.bth4j.admin.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.common.Json;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * JSON admin API over {@link AdminOps} on the JDK HTTP server, optionally with a static dashboard.
 * <p>
 * Sign in with {@code POST /api/v1/session}; the reply sets the {@code bth4j_session} cookie and carries a CSRF
 * token that every later POST and DELETE must send as {@code X-CSRF-Token}. Sessions live in memory, so they end
 * when the server stops. Sessions are sliding: every request pushes the expiry out by the session TTL.
 * <p>
 * Failed logins block the remote address for 10 seconds after 5 failures in a row (HTTP 429 with
 * {@code Retry-After}). Behind a reverse proxy every client shares the proxy's address, so rate-limit there too.
 * <p>
 * The server speaks plain HTTP unless an {@link SSLContext} is given; without one, put it behind TLS or keep it on
 * the loopback interface.
 */
public final class AdminHttpServer {

    private static final Logger log = LoggerFactory.getLogger(AdminHttpServer.class);

    private static final String API = "/api/v1";
    private static final String COOKIE = "bth4j_session";
    private static final String CSRF_HEADER = "X-CSRF-Token";
    private static final int MAX_BODY = 64 * 1024;
    private static final int MAX_SESSIONS = 1000;
    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 200;
    private static final String HTML_CSP =
            "default-src 'self'; script-src 'self' https://cdn.jsdelivr.net 'unsafe-eval'; "
                    + "style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; "
                    + "base-uri 'self'; form-action 'self'; frame-ancestors 'none'";

    private record User(byte[] digest, Role role) {}

    private final AdminOps admin;
    private final String host;
    private final int requestedPort;
    private final Map<String, User> users;
    private final Duration sessionTtl;
    private final StaticFiles staticFiles;
    private final Set<String> corsOrigins;
    private final SSLContext ssl;
    private final Executor executor;
    private final boolean ownExecutor;
    private final Sessions sessions;
    private final LoginThrottle throttle;
    private final byte[] dummyDigest = sha256(randomBytes());
    private HttpServer server;

    private AdminHttpServer(Builder b) {
        this.admin = b.admin;
        this.host = b.host;
        this.requestedPort = b.port;
        this.users = Map.copyOf(b.users);
        this.sessionTtl = b.sessionTtl;
        this.staticFiles = b.staticDir == null ? null : new StaticFiles(b.staticDir);
        this.corsOrigins = Set.copyOf(b.corsOrigins);
        this.ssl = b.ssl;
        this.ownExecutor = b.executor == null;
        this.executor = ownExecutor ? Executors.newVirtualThreadPerTaskExecutor() : b.executor;
        this.sessions = new Sessions(b.clock, b.sessionTtl, MAX_SESSIONS);
        this.throttle = new LoginThrottle(b.clock);
    }

    /** @return a builder with the defaults: 127.0.0.1:8787, session TTL of 12 hours, no static files, no CORS */
    public static Builder builder() {
        return new Builder();
    }

    /** Binds the port and starts serving. */
    public synchronized void start() {
        if (server != null) {
            throw new IllegalStateException("Admin HTTP server is already started");
        }
        try {
            var address = new InetSocketAddress(host, requestedPort);
            if (ssl != null) {
                var https = HttpsServer.create(address, 0);
                https.setHttpsConfigurator(new HttpsConfigurator(ssl));
                server = https;
            } else {
                server = HttpServer.create(address, 0);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot bind the admin HTTP server to " + host + ':' + requestedPort, e);
        }
        server.createContext("/", this::handle);
        server.setExecutor(executor);
        server.start();
        log.info(
                "Admin HTTP server listening on {}://{}:{}",
                ssl == null ? "http" : "https",
                host,
                server.getAddress().getPort());
    }

    /** Stops accepting requests, waiting briefly for running ones. Safe to call when not started. */
    public synchronized void stop() {
        if (server == null) {
            return;
        }
        server.stop(1);
        server = null;
        if (ownExecutor) {
            ((ExecutorService) executor).shutdown();
        }
        log.info("Admin HTTP server stopped");
    }

    /** @return the port actually bound, which differs from the requested one when that was 0 */
    public synchronized int port() {
        if (server == null) {
            throw new IllegalStateException("Admin HTTP server is not started");
        }
        return server.getAddress().getPort();
    }

    private void handle(HttpExchange ex) throws IOException {
        try {
            ex.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
            var origin = applyCors(ex);
            var method = ex.getRequestMethod();
            var path = ex.getRequestURI().getRawPath();
            if ("OPTIONS".equals(method)) {
                preflight(ex, origin);
            } else if (path.equals(API) || path.startsWith(API + "/")) {
                api(ex, method, path.substring(API.length()));
            } else if (path.startsWith("/api/")) {
                throw new HttpError(404, "not found");
            } else if (method.equals("GET")) {
                serveStatic(ex);
            } else {
                throw new HttpError(405, "method not allowed");
            }
        } catch (HttpError e) {
            sendJson(ex, e.status(), Views.obj("error", e.getMessage()));
        } catch (Exception e) {
            log.error(
                    "Admin request {} {} failed",
                    ex.getRequestMethod(),
                    ex.getRequestURI().getRawPath(),
                    e);
            try {
                sendJson(ex, 500, Views.obj("error", "internal error"));
            } catch (Exception ignored) {
                // the response may already be committed; the failure is logged above
            }
        } finally {
            ex.close();
        }
    }

    private String applyCors(HttpExchange ex) {
        var origin = ex.getRequestHeaders().getFirst("Origin");
        if (origin == null || !corsOrigins.contains(origin)) {
            return null;
        }
        var h = ex.getResponseHeaders();
        h.set("Access-Control-Allow-Origin", origin);
        h.set("Access-Control-Allow-Credentials", "true");
        h.add("Vary", "Origin");
        return origin;
    }

    private void preflight(HttpExchange ex, String allowedOrigin) throws IOException {
        if (allowedOrigin != null) {
            var h = ex.getResponseHeaders();
            h.set("Access-Control-Allow-Methods", "GET, POST, DELETE");
            h.set("Access-Control-Allow-Headers", "Content-Type, X-CSRF-Token");
            h.set("Access-Control-Max-Age", "600");
        }
        ex.sendResponseHeaders(204, -1);
    }

    private void api(HttpExchange ex, String method, String rest) throws IOException {
        var segments = segments(rest);
        if (method.equals("POST") && segments.equals(List.of("session"))) {
            login(ex);
            return;
        }
        var session = authenticate(ex);
        var mutation = method.equals("POST") || method.equals("DELETE");
        if (mutation) {
            var sent = ex.getRequestHeaders().getFirst(CSRF_HEADER);
            if (sent == null
                    || !MessageDigest.isEqual(
                            sent.getBytes(StandardCharsets.UTF_8),
                            session.csrf().getBytes(StandardCharsets.UTF_8))) {
                throw new HttpError(403, "missing or wrong CSRF token");
            }
            var logout = method.equals("DELETE") && segments.equals(List.of("session"));
            if (!logout && session.role() != Role.ADMIN) {
                throw new HttpError(403, "this action needs the ADMIN role");
            }
        }
        if (segments.isEmpty() || segments.size() > 3) {
            throw new HttpError(404, "not found");
        }
        var first = segments.get(0);
        var key = method
                + ' '
                + first
                + (segments.size() > 1 ? "/{}" : "")
                + (segments.size() > 2 ? "/" + segments.get(2) : "");
        var param = segments.size() > 1 ? segments.get(1) : null;
        switch (key) {
            case "GET me" ->
                sendJson(
                        ex,
                        200,
                        Views.obj("user", session.user(), "role", session.role().name()));
            case "DELETE session" -> logout(ex, session);
            case "GET health" -> health(ex);
            case "GET overview" -> sendJson(ex, 200, overview());
            case "GET queues" -> sendJson(ex, 200, queues());
            case "POST queues/{}/pause" -> pauseQueue(ex, param, true);
            case "POST queues/{}/resume" -> pauseQueue(ex, param, false);
            case "GET tasks" -> sendJson(ex, 200, tasks(ex));
            case "GET tasks/{}" -> sendJson(ex, 200, Views.task(requireTask(taskId(param))));
            case "GET tasks/{}/payload" -> sendJson(ex, 200, payload(taskId(param)));
            case "POST tasks/{}/retry" -> retry(ex, taskId(param));
            case "POST tasks/{}/cancel" -> cancel(ex, taskId(param));
            case "DELETE tasks/{}" -> delete(ex, taskId(param));
            case "GET schedules" -> sendJson(ex, 200, Views.list(admin.schedules(), Views::schedule));
            case "POST schedules/{}/pause" -> scheduleChange(ex, admin.pauseSchedule(param, true));
            case "POST schedules/{}/resume" -> scheduleChange(ex, admin.pauseSchedule(param, false));
            case "POST schedules/{}/trigger" -> scheduleChange(ex, admin.triggerSchedule(param));
            case "DELETE schedules/{}" -> scheduleChange(ex, admin.deleteSchedule(param));
            case "GET workers" -> sendJson(ex, 200, Views.list(admin.workers(), Views::worker));
            case "GET issues" -> sendJson(ex, 200, Views.list(admin.issues(), Views::issue));
            default -> throw new HttpError(404, "not found");
        }
    }

    private void health(HttpExchange ex) throws IOException {
        var health = admin.health();
        if (health.isPresent()) {
            sendJson(ex, 200, Views.health(health.get()));
        } else {
            ex.sendResponseHeaders(204, -1);
        }
    }

    private void login(HttpExchange ex) throws IOException {
        var address = ex.getRemoteAddress().getAddress().getHostAddress();
        var wait = throttle.retryAfterMillis(address);
        if (wait > 0) {
            ex.getResponseHeaders().set("Retry-After", String.valueOf((wait + 999) / 1000));
            throw new HttpError(429, "too many failed logins, try again later");
        }
        var body = readObject(ex);
        if (!(body.get("user") instanceof String name) || !(body.get("password") instanceof String password)) {
            throw new HttpError(400, "user and password are required");
        }
        var user = users.get(name);
        var expected = user == null ? dummyDigest : user.digest();
        var matches = MessageDigest.isEqual(sha256(password.getBytes(StandardCharsets.UTF_8)), expected);
        if (user == null || !matches) {
            throttle.failure(address);
            throw new HttpError(401, "invalid credentials");
        }
        throttle.success(address);
        var session = sessions.create(name, user.role());
        var cookie = COOKIE + '=' + session.token() + "; HttpOnly; SameSite=Strict; Path=/; Max-Age="
                + sessionTtl.toSeconds() + (ssl != null ? "; Secure" : "");
        ex.getResponseHeaders().add("Set-Cookie", cookie);
        sendJson(
                ex,
                200,
                Views.obj(
                        "csrf",
                        session.csrf(),
                        "user",
                        name,
                        "role",
                        user.role().name()));
    }

    private void logout(HttpExchange ex, Sessions.Session session) throws IOException {
        sessions.remove(session.token());
        ex.getResponseHeaders()
                .add(
                        "Set-Cookie",
                        COOKIE + "=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0" + (ssl != null ? "; Secure" : ""));
        sendJson(ex, 200, Views.obj("ok", true));
    }

    private Sessions.Session authenticate(HttpExchange ex) {
        String token = null;
        for (var header : ex.getRequestHeaders().getOrDefault("Cookie", List.of())) {
            for (var part : header.split(";")) {
                var pair = part.trim();
                if (pair.startsWith(COOKIE + '=')) {
                    token = pair.substring(COOKIE.length() + 1);
                }
            }
        }
        var session = sessions.find(token);
        if (session == null) {
            throw new HttpError(401, "not signed in");
        }
        return session;
    }

    private Map<String, Object> overview() {
        var counts = new LinkedHashMap<String, Object>();
        var total = StateCounts.ZERO;
        for (var e : admin.counts().entrySet()) {
            counts.put(e.getKey(), Views.counts(e.getValue()));
            total = Views.sum(total, e.getValue());
        }
        return Views.obj(
                "counts", counts,
                "totals", Views.counts(total),
                "workers", admin.workers().size(),
                "issues", admin.issues().size(),
                "health", admin.health().map(Views::health).orElse(null));
    }

    private List<Object> queues() {
        var counts = admin.counts();
        return Views.list(admin.queues(), q -> Views.queue(q, counts.getOrDefault(q.name(), StateCounts.ZERO)));
    }

    private void pauseQueue(HttpExchange ex, String name, boolean paused) throws IOException {
        if (admin.queues().stream().noneMatch(q -> q.name().equals(name))) {
            throw new HttpError(404, "unknown queue");
        }
        admin.pauseQueue(name, paused);
        sendJson(ex, 200, Views.obj("ok", true));
    }

    private List<Object> tasks(HttpExchange ex) {
        var query = query(ex);
        var stateName = query.get("state");
        if (stateName == null) {
            throw new HttpError(400, "state is required");
        }
        TaskState state;
        try {
            state = TaskState.valueOf(stateName.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new HttpError(400, "unknown state");
        }
        var after = query.containsKey("after") ? parseLong(query.get("after"), "after") : 0L;
        var limit = DEFAULT_LIMIT;
        if (query.containsKey("limit")) {
            limit = (int) Math.min(parseLong(query.get("limit"), "limit"), MAX_LIMIT);
            if (limit < 1) {
                throw new HttpError(400, "limit must be at least 1");
            }
        }
        var queue = query.get("queue");
        return Views.list(
                admin.tasks(state, queue == null || queue.isEmpty() ? null : queue, after, limit), Views::task);
    }

    private Map<String, Object> payload(long id) {
        return Views.obj("payload", requireTask(id).data());
    }

    private void retry(HttpExchange ex, long id) throws IOException {
        var revision = revision(readObject(ex).get("revision"));
        changeTask(ex, id, admin.retry(id, revision));
    }

    private void cancel(HttpExchange ex, long id) throws IOException {
        var body = readObject(ex);
        var revision = revision(body.get("revision"));
        var reason = body.get("reason");
        if (reason != null && !(reason instanceof String)) {
            throw new HttpError(400, "reason must be a string");
        }
        var outcome = admin.cancel(id, revision, (String) reason);
        if (outcome == CancelOutcome.STALE) {
            changeTask(ex, id, false);
        } else {
            sendJson(ex, 200, Views.obj("ok", true, "outcome", outcome.name()));
        }
    }

    private void delete(HttpExchange ex, long id) throws IOException {
        var raw = query(ex).get("revision");
        if (raw == null) {
            throw new HttpError(400, "revision is required");
        }
        changeTask(ex, id, admin.delete(id, revision(raw)));
    }

    /** A false result is either a stale revision or a task that is gone; only the lookup tells which. */
    private void changeTask(HttpExchange ex, long id, boolean changed) throws IOException {
        if (!changed) {
            if (admin.task(id).isEmpty()) {
                throw new HttpError(404, "unknown task");
            }
            throw new HttpError(409, "the task changed since you loaded it");
        }
        sendJson(ex, 200, Views.obj("ok", true));
    }

    private void scheduleChange(HttpExchange ex, boolean found) throws IOException {
        if (!found) {
            throw new HttpError(404, "unknown schedule");
        }
        sendJson(ex, 200, Views.obj("ok", true));
    }

    private today.bonfire.oss.bth4j.TaskView requireTask(long id) {
        return admin.task(id).orElseThrow(() -> new HttpError(404, "unknown task"));
    }

    private static long taskId(String raw) {
        return parseLong(raw, "id");
    }

    private static long parseLong(String raw, String what) {
        try {
            var value = Long.parseLong(raw);
            if (value < 0) {
                throw new HttpError(400, what + " must not be negative");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new HttpError(400, what + " is not a number");
        }
    }

    private static int revision(Object raw) {
        try {
            if (raw instanceof String s) {
                return Integer.parseInt(s);
            }
            if (raw instanceof Long l) {
                return Math.toIntExact(l);
            }
        } catch (NumberFormatException | ArithmeticException e) {
            throw new HttpError(400, "revision is not a valid number");
        }
        throw new HttpError(400, "revision is required");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readObject(HttpExchange ex) throws IOException {
        var bytes = ex.getRequestBody().readNBytes(MAX_BODY + 1);
        if (bytes.length > MAX_BODY) {
            throw new HttpError(400, "request body too large");
        }
        try {
            if (Json.parse(new String(bytes, StandardCharsets.UTF_8)) instanceof Map<?, ?> map) {
                return (Map<String, Object>) map;
            }
        } catch (IllegalArgumentException e) {
            throw new HttpError(400, "malformed JSON");
        }
        throw new HttpError(400, "a JSON object is required");
    }

    private static List<String> segments(String rest) {
        var out = new ArrayList<String>();
        for (var part : rest.split("/")) {
            if (!part.isEmpty()) {
                out.add(decode(part));
            }
        }
        return out;
    }

    private static Map<String, String> query(HttpExchange ex) {
        var out = new HashMap<String, String>();
        var raw = ex.getRequestURI().getRawQuery();
        if (raw != null) {
            for (var pair : raw.split("&")) {
                var eq = pair.indexOf('=');
                if (eq > 0) {
                    out.put(decode(pair.substring(0, eq)), decode(pair.substring(eq + 1)));
                }
            }
        }
        return out;
    }

    private static String decode(String s) {
        try {
            return URLDecoder.decode(s.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new HttpError(400, "malformed URL encoding");
        }
    }

    private void serveStatic(HttpExchange ex) throws IOException {
        if (staticFiles == null) {
            if (ex.getRequestURI().getRawPath().equals("/")) {
                sendJson(ex, 200, Views.obj("name", "bth4j admin", "api", API));
                return;
            }
            throw new HttpError(404, "not found");
        }
        var resource = staticFiles.find(ex.getRequestURI().getPath());
        if (resource == null) {
            throw new HttpError(404, "not found");
        }
        var h = ex.getResponseHeaders();
        h.set("Content-Type", resource.contentType());
        h.set("Cache-Control", "no-cache");
        if (resource.html()) {
            h.set("Content-Security-Policy", HTML_CSP);
        }
        ex.sendResponseHeaders(200, resource.body().length);
        ex.getResponseBody().write(resource.body());
    }

    private void sendJson(HttpExchange ex, int status, Object body) throws IOException {
        var bytes = Json.write(body).getBytes(StandardCharsets.UTF_8);
        var h = ex.getResponseHeaders();
        h.set("Content-Type", "application/json; charset=utf-8");
        h.set("Cache-Control", "no-store");
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
    }

    private static byte[] randomBytes() {
        var bytes = new byte[32];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }

    private static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every JDK", e);
        }
    }

    /** Collects the settings of an {@link AdminHttpServer}. */
    public static final class Builder {
        private AdminOps admin;
        private String host = "127.0.0.1";
        private int port = 8787;
        private final Map<String, User> users = new LinkedHashMap<>();
        private Duration sessionTtl = Duration.ofHours(12);
        private Path staticDir;
        private final List<String> corsOrigins = new ArrayList<>();
        private SSLContext ssl;
        private Executor executor;
        private Clock clock = Clock.systemUTC();

        private Builder() {}

        /** The operations the API exposes; required. */
        public Builder admin(AdminOps admin) {
            this.admin = admin;
            return this;
        }

        /** Where to listen; the default is 127.0.0.1:8787. Port 0 picks a free port, see {@link AdminHttpServer#port()}. */
        public Builder bind(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        /** Adds a user; at least one is required. Only the SHA-256 digest of the password is kept. */
        public Builder user(String name, String password, Role role) {
            if (name == null || name.isEmpty() || password == null || role == null) {
                throw new IllegalArgumentException("name, password and role are required");
            }
            users.put(name, new User(sha256(password.getBytes(StandardCharsets.UTF_8)), role));
            return this;
        }

        /** How long a session survives without a request; the default is 12 hours. */
        public Builder sessionTtl(Duration ttl) {
            this.sessionTtl = ttl;
            return this;
        }

        /** Serves the files below this directory for every GET outside {@code /api/}. */
        public Builder staticDir(Path dir) {
            this.staticDir = dir;
            return this;
        }

        /** Origins, such as {@code https://admin.example.com}, that may call the API from a browser. */
        public Builder corsOrigin(String... origins) {
            corsOrigins.addAll(List.of(origins));
            return this;
        }

        /** Serves HTTPS with this context and marks the session cookie Secure. */
        public Builder https(SSLContext context) {
            this.ssl = context;
            return this;
        }

        /** Runs requests on this executor; the default is a virtual thread per request, shut down by {@link AdminHttpServer#stop()}. */
        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /** @throws IllegalStateException when the admin operations or all users are missing */
        public AdminHttpServer build() {
            if (admin == null) {
                throw new IllegalStateException("admin(AdminOps) is required");
            }
            if (users.isEmpty()) {
                throw new IllegalStateException("at least one user is required");
            }
            if (sessionTtl == null || sessionTtl.isZero() || sessionTtl.isNegative()) {
                throw new IllegalStateException("sessionTtl must be positive");
            }
            return new AdminHttpServer(this);
        }
    }
}
