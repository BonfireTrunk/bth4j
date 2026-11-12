package today.bonfire.oss.bth4j.admin.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
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
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.StateCounts;
import today.bonfire.oss.bth4j.TaskRequest;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.internal.Json;

/**
 * JSON admin API over {@link AdminOps} on the JDK HTTP server, optionally with a static dashboard.
 * <p>
 * Sign in with {@code POST /api/v1/session}; the reply sets the {@code bth4j_session} cookie and carries a CSRF
 * token that every later POST and DELETE must send as {@code X-CSRF-Token}. Sessions live in memory, so they end
 * when the server stops. A session ends its lifetime after the sign-in; requests do not extend it.
 * <p>
 * Failed logins are counted by where they come from and by the user they name: the fifth failure blocks that
 * address, or that user name, for 10 seconds (HTTP 429 with {@code Retry-After}). Behind a reverse proxy every
 * client shares the proxy's address unless {@link Builder#addTrustedProxy(String)} names the proxy.
 * <p>
 * The server speaks plain HTTP unless an {@link SSLContext} is given; without one, put it behind TLS or keep it on
 * the loopback interface. The session cookie is marked Secure when an {@link SSLContext} is given; behind a
 * TLS-terminating proxy set {@link Builder#secureCookie(boolean)} instead.
 * <p>
 * An instance is one-shot: after {@link #stop()} it cannot be started again, build a new one.
 */
public final class AdminHttpServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AdminHttpServer.class);

    private static final String API = "/api/v1";
    private static final String COOKIE = "bth4j_session";
    private static final String CSRF_HEADER = "X-CSRF-Token";
    private static final int MAX_BODY = 64 * 1024;
    private static final int MAX_SESSIONS = 1000;
    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 200;
    private static final String HTML_CSP = "default-src 'self'; script-src 'self' 'unsafe-eval'; "
            + "style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; "
            + "base-uri 'self'; form-action 'self'; frame-ancestors 'none'";

    private record User(byte[] salt, byte[] digest, Role role) {
        static User of(String password, Role role) {
            var salt = randomBytes();
            return new User(salt, AdminHttpServer.digest(salt, password), role);
        }
    }

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
    private final LoginThrottle addressThrottle;
    private final LoginThrottle userThrottle;
    private final ClientAddress clientAddress;
    /** Keys the hash the user throttle counts by, so that it holds no user names, also none that were only guessed. */
    private final byte[] userKeySalt = randomBytes();

    private final AtomicBoolean forwardedHeaderIgnored = new AtomicBoolean();
    // an unknown user is checked like a known one, so the reply time does not tell which names exist
    private final User dummyUser = User.of(HexFormat.of().formatHex(randomBytes()), Role.VIEWER);
    private final boolean secureCookie;
    private HttpServer server;
    private boolean stopped;
    private final ReentrantLock lifecycle = new ReentrantLock();

    private AdminHttpServer(Builder b) {
        this.admin = b.admin;
        this.host = b.host;
        this.requestedPort = b.port;
        this.users = Map.copyOf(b.users);
        this.sessionTtl = b.sessionTtl;
        this.staticFiles = b.staticDir == null ? null : new StaticFiles(b.staticDir);
        this.corsOrigins = Set.copyOf(b.corsOrigins);
        this.ssl = b.ssl;
        this.secureCookie = b.secureCookie != null ? b.secureCookie : b.ssl != null;
        this.ownExecutor = b.executor == null;
        this.executor = ownExecutor ? Executors.newVirtualThreadPerTaskExecutor() : b.executor;
        this.sessions = new Sessions(b.clock, b.sessionTtl, MAX_SESSIONS);
        this.addressThrottle = new LoginThrottle(b.clock);
        this.userThrottle = new LoginThrottle(b.clock);
        this.clientAddress = ClientAddress.trusting(b.trustedProxies);
    }

    /** @return a builder with the defaults: 127.0.0.1:8787, session TTL of 12 hours, no static files, no CORS */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Binds the port and starts serving. Calling it on a server that is started does nothing.
     *
     * @throws IllegalStateException when {@link #stop()} or {@link #close()} was called before, also on a server
     *     that was never started: a stopped server stays stopped, build a new one
     */
    public void start() {
        lifecycle.lock();
        try {
            if (stopped) {
                throw new IllegalStateException(
                        "Admin HTTP server was stopped and cannot be started again; build a new server");
            }
            if (server != null) {
                log.debug("Admin HTTP server is already started");
                return;
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
        } finally {
            lifecycle.unlock();
        }
    }

    /**
     * Stops accepting requests, waiting up to one second for running ones. A stopped server cannot be started
     * again. The call can come before {@link #start()} and can be repeated.
     */
    public void stop() {
        lifecycle.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;
            if (server != null) {
                server.stop(1);
                server = null;
            }
            if (ownExecutor) {
                ((ExecutorService) executor).shutdown();
            }
            log.info("Admin HTTP server stopped");
        } finally {
            lifecycle.unlock();
        }
    }

    /** Same as {@link #stop()}, for try-with-resources. */
    @Override
    public void close() {
        stop();
    }

    /** @return the port actually bound, which differs from the requested one when that was 0 */
    public int port() {
        lifecycle.lock();
        try {
            if (server == null) {
                throw new IllegalStateException("Admin HTTP server is not started");
            }
            return server.getAddress().getPort();
        } finally {
            lifecycle.unlock();
        }
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
            var body = Views.obj("error", e.getMessage());
            if (e.code() != null) body.put("code", e.code());
            sendJson(ex, e.status(), body);
        } catch (TaskValidationException e) {
            sendJson(ex, 400, Views.obj("error", e.getMessage()));
        } catch (UnreadableTaskException e) {
            log.warn(
                    "Admin request {} {}: task {} has a stored value that cannot be read",
                    ex.getRequestMethod(),
                    routeTemplate(ex.getRequestURI().getRawPath()),
                    e.taskId());
            var body = Views.obj("error", "the task has a stored value that cannot be read");
            body.put("code", "UNREADABLE_TASK");
            sendJson(ex, 422, body);
        } catch (Exception e) {
            log.error(
                    "Admin request {} {} failed",
                    ex.getRequestMethod(),
                    routeTemplate(ex.getRequestURI().getRawPath()),
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
        var logout = method.equals("DELETE") && segments.equals(List.of("session"));
        var mutation = method.equals("POST") || method.equals("DELETE");
        if (mutation) {
            var sent = ex.getRequestHeaders().getFirst(CSRF_HEADER);
            if (sent == null
                    || !MessageDigest.isEqual(
                            sent.getBytes(StandardCharsets.UTF_8),
                            session.csrf().getBytes(StandardCharsets.UTF_8))) {
                throw new HttpError(403, "missing or wrong CSRF token");
            }
            if (!logout && session.role() != Role.ADMIN) {
                throw new HttpError(403, "this action needs the ADMIN role");
            }
        }
        var byKey = segments.size() >= 2
                && segments.get(0).equals("tasks")
                && segments.get(1).equals("by-key");
        if (byKey) {
            byKey(ex, method, segments);
            return;
        }
        if (segments.isEmpty() || segments.size() > 3) {
            throw new HttpError(404, "not found");
        }
        var first = segments.get(0);
        // an encoded slash in a single segment would otherwise spell a route that takes an id, without one
        if (first.indexOf('/') >= 0) {
            throw new HttpError(404, "not found");
        }
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
                        Views.obj("user", session.user(), "role", session.role().name(), "csrf", session.csrf()));
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
            case "GET runners" -> sendJson(ex, 200, Views.list(admin.runners(), Views::runner));
            case "GET unserved" -> sendJson(ex, 200, Views.list(admin.unservedWork(), Views::unserved));
            case "GET issues" -> sendJson(ex, 200, Views.list(admin.issues(), Views::issue));
            default -> throw new HttpError(404, "not found");
        }
    }

    /** The key is a query value: a path cannot carry every key, a client resolves the segments "." and "..". */
    private void byKey(HttpExchange ex, String method, List<String> segments) throws IOException {
        var read = segments.size() == 2 && method.equals("GET");
        var cancel =
                segments.size() == 3 && method.equals("POST") && segments.get(2).equals("cancel");
        if (!read && !cancel) {
            throw new HttpError(404, "not found");
        }
        var key = query(ex).get("key");
        if (key == null) {
            throw new HttpError(400, "key is required");
        }
        try {
            TaskRequest.requireUniqueKey(key);
        } catch (TaskValidationException e) {
            throw new HttpError(400, e.getMessage());
        }
        if (read) {
            var task = admin.taskByKey(key).orElseThrow(() -> new HttpError(404, "no task holds this key"));
            sendJson(ex, 200, Views.task(task));
        } else {
            var reason = readObject(ex).get("reason");
            if (reason != null && !(reason instanceof String)) {
                throw new HttpError(400, "reason must be a string");
            }
            var outcome = admin.cancelByKey(key, (String) reason);
            switch (outcome) {
                case NOT_FOUND -> throw notFound("no task holds this key");
                case FINISHED -> throw wrongState("the task holding this key is already finished");
                case STALE -> throw stale();
                default -> sendJson(ex, 200, Views.obj("ok", true, "outcome", outcome.name()));
            }
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
        var address = ClientAddress.throttleKey(clientAddress(ex));
        var wait = addressThrottle.attempt(address);
        if (wait > 0) {
            throw tooManyLogins(ex, wait);
        }
        var body = readObject(ex);
        if (!(body.get("user") instanceof String name) || !(body.get("password") instanceof String password)) {
            throw new HttpError(400, "user and password are required");
        }
        // counted for every name, known or not, so that a block does not tell which users exist
        var userKey = HexFormat.of().formatHex(digest(userKeySalt, name));
        wait = userThrottle.attempt(userKey);
        if (wait > 0) {
            addressThrottle.takeBack(address);
            throw tooManyLogins(ex, wait);
        }
        var user = users.get(name);
        var checked = user == null ? dummyUser : user;
        var matches = MessageDigest.isEqual(digest(checked.salt(), password), checked.digest());
        if (user == null || !matches) {
            throw new HttpError(401, "invalid credentials");
        }
        addressThrottle.takeBack(address);
        userThrottle.takeBack(userKey);
        var session = sessions.create(name, user.role());
        ex.getResponseHeaders().add("Set-Cookie", sessionCookie(session.token(), sessionTtl.toSeconds()));
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

    private static HttpError tooManyLogins(HttpExchange ex, long waitMillis) {
        ex.getResponseHeaders().set("Retry-After", String.valueOf((waitMillis + 999) / 1000));
        return new HttpError(429, "too many failed logins, try again later");
    }

    private InetAddress clientAddress(HttpExchange ex) {
        var peer = ex.getRemoteAddress().getAddress();
        var forwardedFor = ex.getRequestHeaders().get("X-Forwarded-For");
        if (forwardedFor != null && !clientAddress.trusts(peer) && forwardedHeaderIgnored.compareAndSet(false, true)) {
            log.warn(
                    "A login from {} carried X-Forwarded-For and the header was ignored, because that address is"
                            + " not a trusted proxy. When the server is behind a proxy, name it with addTrustedProxy,"
                            + " or all clients are counted as one. This is logged once.",
                    peer.getHostAddress());
        }
        return clientAddress.resolve(peer, forwardedFor);
    }

    private void logout(HttpExchange ex, Sessions.Session session) throws IOException {
        sessions.remove(session.token());
        ex.getResponseHeaders().set("Set-Cookie", sessionCookie("", 0));
        sendJson(ex, 200, Views.obj("ok", true));
    }

    private String sessionCookie(String token, long maxAgeSeconds) {
        return COOKIE + '=' + token + "; HttpOnly; SameSite=Strict; Path=/; Max-Age=" + maxAgeSeconds
                + (secureCookie ? "; Secure" : "");
    }

    /** The shape of a route with its path parameters, such as a unique key, replaced; safe to log. */
    static String routeTemplate(String rawPath) {
        if (!rawPath.equals(API) && !rawPath.startsWith(API + "/")) {
            return "(non-api path)";
        }
        var parts = new ArrayList<String>();
        for (var part : rawPath.substring(API.length()).split("/")) {
            if (!part.isEmpty()) {
                parts.add(part);
            }
        }
        var byKey = parts.size() >= 2
                && parts.get(0).equals("tasks")
                && parts.get(1).equals("by-key");
        if (!byKey && parts.size() >= 2) {
            parts.set(1, "{}");
        }
        // the routes of 4.x carried the unique key as the segment after by-key
        if (byKey && parts.size() >= 3 && !(parts.size() == 3 && parts.get(2).equals("cancel"))) {
            parts.set(2, "{}");
        }
        return API + '/' + String.join("/", parts.subList(0, Math.min(parts.size(), 4)));
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
                "runners", admin.runners().size(),
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
        try {
            admin.pauseQueue(name, paused);
        } catch (TaskValidationException e) {
            throw new HttpError(404, "unknown queue");
        }
        sendJson(ex, 200, Views.obj("ok", true));
    }

    private Object tasks(HttpExchange ex) {
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
        var limit = DEFAULT_LIMIT;
        if (query.containsKey("limit")) {
            limit = (int) Math.min(parseLong(query.get("limit"), "limit"), MAX_LIMIT);
            if (limit < 1) {
                throw new HttpError(400, "limit must be at least 1");
            }
        }
        var queue = query.get("queue");
        var cursor = query.get("cursor");
        var cursorOrNull = cursor == null || cursor.isEmpty() ? null : cursor;
        var page = queue == null || queue.isEmpty()
                ? admin.tasks(state, cursorOrNull, limit)
                : admin.tasks(state, queue, cursorOrNull, limit);
        return Views.obj("items", Views.list(page.items(), Views::task), "next", page.next());
    }

    private Map<String, Object> payload(long id) {
        return Views.obj("payload", requireTask(id).encodedPayload());
    }

    private void retry(HttpExchange ex, long id) throws IOException {
        var revision = revision(readObject(ex).get("revision"));
        switch (admin.retry(id, revision)) {
            case NOT_FOUND -> throw notFound("there is no such task");
            case STALE -> throw stale();
            case NOT_RETRYABLE -> throw wrongState("only a dead, cancelled or expired task can be retried");
            case RETRIED -> sendJson(ex, 200, Views.obj("ok", true));
        }
    }

    private void cancel(HttpExchange ex, long id) throws IOException {
        var body = readObject(ex);
        var revision = revision(body.get("revision"));
        var reason = body.get("reason");
        if (reason != null && !(reason instanceof String)) {
            throw new HttpError(400, "reason must be a string");
        }
        var outcome = admin.cancel(id, revision, (String) reason);
        switch (outcome) {
            case NOT_FOUND -> throw notFound("there is no such task");
            case STALE -> throw stale();
            case FINISHED -> throw wrongState("the task has finished and cannot be cancelled");
            default -> sendJson(ex, 200, Views.obj("ok", true, "outcome", outcome.name()));
        }
    }

    private void delete(HttpExchange ex, long id) throws IOException {
        var raw = query(ex).get("revision");
        if (raw == null) {
            throw new HttpError(400, "revision is required");
        }
        switch (admin.delete(id, revision(raw))) {
            case NOT_FOUND -> throw notFound("there is no such task");
            case STALE -> throw stale();
            case LEASED -> throw wrongState("a task that is running cannot be deleted");
            case DELETED -> sendJson(ex, 200, Views.obj("ok", true));
        }
    }

    private static HttpError notFound(String message) {
        return new HttpError(404, message, "NOT_FOUND");
    }

    private static HttpError stale() {
        return new HttpError(409, "the task changed since it was loaded", "STALE");
    }

    private static HttpError wrongState(String message) {
        return new HttpError(409, message, "WRONG_STATE");
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
                    out.put(decodeQuery(pair.substring(0, eq)), decodeQuery(pair.substring(eq + 1)));
                }
            }
        }
        return out;
    }

    /** Decodes a path segment, where a plus sign stands for itself. */
    private static String decode(String s) {
        return decodeQuery(s.replace("+", "%2B"));
    }

    /** Decodes a query name or value, where a plus sign stands for a space. */
    private static String decodeQuery(String s) {
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
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

    private static byte[] digest(byte[] salt, String password) {
        try {
            var sha3 = MessageDigest.getInstance("SHA3-384");
            sha3.update(salt);
            return sha3.digest(password.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA3-384 is provided by the JDK since Java 9", e);
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
        private Boolean secureCookie;
        private final List<String> trustedProxies = new ArrayList<>();
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

        /**
         * Adds a user; at least one is required. Only a random salt and the SHA3-384 digest of the salt and password
         * are kept.
         *
         * @throws TaskConfigurationException when a part is missing or a user with that name was added before
         */
        public Builder addUser(String name, String password, Role role) {
            if (name == null || name.isEmpty() || password == null || password.isEmpty() || role == null) {
                throw new TaskConfigurationException("name, password and role are required");
            }
            if (users.putIfAbsent(name, User.of(password, role)) != null) {
                throw new TaskConfigurationException("a user with this name was added before");
            }
            return this;
        }

        /**
         * How long a session lasts, counted from the sign-in; the default is 12 hours. Requests do not make it last
         * longer.
         */
        public Builder sessionTtl(Duration ttl) {
            this.sessionTtl = ttl;
            return this;
        }

        /** Serves the files below this directory for every GET outside {@code /api/}. */
        public Builder staticDir(Path dir) {
            this.staticDir = dir;
            return this;
        }

        /** Adds an origin, such as {@code https://admin.example.com}, that may call the API from a browser. */
        public Builder addCorsOrigin(String origin) {
            if (origin == null || origin.isBlank()) throw new TaskConfigurationException("origin is required");
            corsOrigins.add(origin);
            return this;
        }

        /** Serves HTTPS with this context and, unless {@link #secureCookie(boolean)} says otherwise, marks the session cookie Secure. */
        public Builder https(SSLContext context) {
            this.ssl = context;
            return this;
        }

        /**
         * Whether the session cookie carries the Secure attribute. The default follows {@link #https}: Secure when a
         * context is given. Set it to true behind a TLS-terminating proxy, where the server itself speaks plain HTTP.
         */
        public Builder secureCookie(boolean secure) {
            this.secureCookie = secure;
            return this;
        }

        /**
         * Adds a reverse proxy in front of the server, as an address ({@code 10.0.0.5}, {@code ::1}) or a
         * range ({@code 10.0.0.0/8}). For a request that arrives from one of them the login throttle counts by
         * the last address in {@code X-Forwarded-For} that is not a trusted proxy, so the proxy must set or
         * append that header itself. Without this setting the header is ignored and every client behind a proxy
         * counts as the proxy's address. Name only proxies: a request from any address named here can state the
         * address it is counted by.
         *
         * @throws TaskConfigurationException when it is not an IP address or range
         */
        public Builder addTrustedProxy(String proxy) {
            var all = new ArrayList<>(trustedProxies);
            all.add(proxy);
            ClientAddress.trusting(all);
            trustedProxies.add(proxy);
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

        /** @throws TaskConfigurationException when the admin operations or all users are missing */
        public AdminHttpServer build() {
            if (admin == null) {
                throw new TaskConfigurationException("admin(AdminOps) is required");
            }
            if (users.isEmpty()) {
                throw new TaskConfigurationException("at least one user is required");
            }
            if (sessionTtl == null || sessionTtl.isZero() || sessionTtl.isNegative()) {
                throw new TaskConfigurationException("sessionTtl must be positive");
            }
            return new AdminHttpServer(this);
        }
    }
}
