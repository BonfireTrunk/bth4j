package today.bonfire.oss.bth4j.admin.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import today.bonfire.oss.bth4j.Issue;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.ScheduleView;
import today.bonfire.oss.bth4j.common.Json;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

class AdminHttpServerTest {

    private static final class MutableClock extends Clock {
        private final AtomicLong millis = new AtomicLong(1_800_000_000_000L);

        void advance(Duration d) {
            millis.addAndGet(d.toMillis());
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis.get());
        }
    }

    private record Login(String cookie, String csrf) {}

    @TempDir
    Path tmp;

    private final FakeAdminOps ops = new FakeAdminOps();
    private final MutableClock clock = new MutableClock();
    private final HttpClient http = HttpClient.newHttpClient();
    private AdminHttpServer server;

    @BeforeEach
    void setUp() {
        ops.queues.add(new QueueInfo(1, "default", false));
        ops.counts.put("default", new StateCounts(3, 1, 2, 10, 4, 0, 1));
        ops.counts.put("mail", new StateCounts(1, 0, 0, 5, 0, 2, 0));
        ops.tasks.put(1L, FakeAdminOps.task(1, 7, TaskState.DEAD, "default", "{\"to\":\"a\"}"));
        ops.tasks.put(2L, FakeAdminOps.task(2, 1, TaskState.DEAD, "mail", null));
        ops.tasks.put(3L, FakeAdminOps.task(3, 1, TaskState.QUEUED, "default", "x"));
        ops.tasks.put(4L, FakeAdminOps.task(4, 2, TaskState.DEAD, "default", "y"));
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    private AdminHttpServer.Builder builder() {
        return AdminHttpServer.builder()
                .admin(ops)
                .bind("127.0.0.1", 0)
                .user("alice", "s3cret", Role.ADMIN)
                .user("victor", "view", Role.VIEWER)
                .clock(clock);
    }

    private void start(AdminHttpServer.Builder b) {
        server = b.build();
        server.start();
    }

    private void start() {
        start(builder());
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + server.port() + path);
    }

    private HttpResponse<String> send(String method, String path, Login login, String body, String... headers)
            throws Exception {
        var req = HttpRequest.newBuilder(uri(path))
                .method(
                        method,
                        body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (login != null) {
            req.header("Cookie", login.cookie());
        }
        for (var i = 0; i < headers.length; i += 2) {
            req.header(headers[i], headers[i + 1]);
        }
        return http.send(req.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path, Login login) throws Exception {
        return send("GET", path, login, null);
    }

    private HttpResponse<String> mutate(String method, String path, Login login, String body) throws Exception {
        return send(method, path, login, body, "X-CSRF-Token", login.csrf());
    }

    private HttpResponse<String> rawLogin(String user, String password) throws Exception {
        return send("POST", "/api/v1/session", null, Json.write(Map.of("user", user, "password", password)));
    }

    private Login login(String user, String password) throws Exception {
        var res = rawLogin(user, password);
        assertThat(res.statusCode()).isEqualTo(200);
        var cookie = res.headers().firstValue("Set-Cookie").orElseThrow();
        return new Login(
                cookie.substring(0, cookie.indexOf(';')), (String) json(res).get("csrf"));
    }

    private Login admin() throws Exception {
        return login("alice", "s3cret");
    }

    private Login viewer() throws Exception {
        return login("victor", "view");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> json(HttpResponse<String> res) {
        return (Map<String, Object>) Json.parse(res.body());
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> jsonList(HttpResponse<String> res) {
        return (List<Map<String, Object>>) Json.parse(res.body());
    }

    @Test
    void loginSetsHardenedCookieAndReturnsCsrfUserAndRole() throws Exception {
        start();

        var res = rawLogin("alice", "s3cret");

        assertThat(res.statusCode()).isEqualTo(200);
        var cookie = res.headers().firstValue("Set-Cookie").orElseThrow();
        assertThat(cookie).startsWith("bth4j_session=").contains("HttpOnly", "SameSite=Strict", "Path=/");
        assertThat(cookie).doesNotContain("Secure");
        var body = json(res);
        assertThat(body).containsEntry("user", "alice").containsEntry("role", "ADMIN");
        assertThat((String) body.get("csrf")).hasSize(43);
        assertThat(res.headers().firstValue("X-Content-Type-Options")).contains("nosniff");
    }

    @Test
    void loginIsDeniedForWrongPasswordUnknownUserAndBadBody() throws Exception {
        start();

        assertThat(rawLogin("alice", "nope").statusCode()).isEqualTo(401);
        assertThat(rawLogin("nobody", "s3cret").statusCode()).isEqualTo(401);
        assertThat(send("POST", "/api/v1/session", null, "{\"user\":\"alice\"}").statusCode())
                .isEqualTo(400);
        assertThat(send("POST", "/api/v1/session", null, "not json").statusCode())
                .isEqualTo(400);
        assertThat(rawLogin("alice", "nope").headers().firstValue("Set-Cookie")).isEmpty();
    }

    @Test
    void repeatedFailedLoginsBlockTheAddressUntilTheDelayPasses() throws Exception {
        start();
        for (var i = 0; i < 5; i++) {
            assertThat(rawLogin("alice", "bad").statusCode()).isEqualTo(401);
        }

        var blocked = rawLogin("alice", "s3cret");
        assertThat(blocked.statusCode()).isEqualTo(429);
        assertThat(blocked.headers().firstValue("Retry-After")).isPresent();

        clock.advance(Duration.ofSeconds(11));
        assertThat(rawLogin("alice", "s3cret").statusCode()).isEqualTo(200);
    }

    @Test
    void apiWithoutOrWithUnknownSessionIs401() throws Exception {
        start();

        assertThat(get("/api/v1/me", null).statusCode()).isEqualTo(401);
        assertThat(get("/api/v1/overview", new Login("bth4j_session=bogus", "x"))
                        .statusCode())
                .isEqualTo(401);
        assertThat(send("POST", "/api/v1/queues/default/pause", null, null).statusCode())
                .isEqualTo(401);
    }

    @Test
    void meReportsUserAndRole() throws Exception {
        start();

        assertThat(json(get("/api/v1/me", viewer())))
                .containsEntry("user", "victor")
                .containsEntry("role", "VIEWER");
    }

    @Test
    void logoutEndsTheSessionAndViewersMayLogOut() throws Exception {
        start();
        var login = viewer();

        assertThat(mutate("DELETE", "/api/v1/session", login, null).statusCode())
                .isEqualTo(200);

        assertThat(get("/api/v1/me", login).statusCode()).isEqualTo(401);
    }

    @Test
    void everyMutationIsForbiddenForViewers() throws Exception {
        ops.schedules.add(schedule("nightly"));
        start();
        var login = viewer();
        var revisionBody = "{\"revision\":\"1\"}";

        var responses = List.of(
                mutate("POST", "/api/v1/queues/default/pause", login, null),
                mutate("POST", "/api/v1/queues/default/resume", login, null),
                mutate("POST", "/api/v1/tasks/1/retry", login, revisionBody),
                mutate("POST", "/api/v1/tasks/1/cancel", login, revisionBody),
                mutate("DELETE", "/api/v1/tasks/1?revision=7", login, null),
                mutate("POST", "/api/v1/schedules/nightly/pause", login, null),
                mutate("POST", "/api/v1/schedules/nightly/resume", login, null),
                mutate("POST", "/api/v1/schedules/nightly/trigger", login, null),
                mutate("DELETE", "/api/v1/schedules/nightly", login, null));

        assertThat(responses).extracting(HttpResponse::statusCode).containsOnly(403);
        assertThat(ops.calls).isEmpty();
    }

    @Test
    void mutationsWithoutOrWithWrongCsrfTokenAreForbidden() throws Exception {
        start();
        var login = admin();

        assertThat(send("POST", "/api/v1/tasks/1/retry", login, "{\"revision\":\"7\"}")
                        .statusCode())
                .isEqualTo(403);
        assertThat(send("POST", "/api/v1/tasks/1/retry", login, "{\"revision\":\"7\"}", "X-CSRF-Token", "wrong")
                        .statusCode())
                .isEqualTo(403);
        assertThat(send("DELETE", "/api/v1/tasks/1?revision=7", login, null).statusCode())
                .isEqualTo(403);
        assertThat(send("DELETE", "/api/v1/session", login, null).statusCode()).isEqualTo(403);
        assertThat(ops.calls).isEmpty();
    }

    @Test
    void csrfTokenOfAnotherSessionIsRejected() throws Exception {
        start();
        var first = admin();
        var second = admin();

        var res = send("POST", "/api/v1/tasks/1/retry", first, "{\"revision\":\"7\"}", "X-CSRF-Token", second.csrf());

        assertThat(res.statusCode()).isEqualTo(403);
    }

    @Test
    void sessionExpiresAfterTheTtlWithoutRequestsAndSlidesWhileActive() throws Exception {
        start(builder().sessionTtl(Duration.ofMinutes(10)));
        var login = admin();

        clock.advance(Duration.ofMinutes(6));
        assertThat(get("/api/v1/me", login).statusCode()).isEqualTo(200);
        clock.advance(Duration.ofMinutes(6));
        assertThat(get("/api/v1/me", login).statusCode()).isEqualTo(200);
        clock.advance(Duration.ofMinutes(10));

        assertThat(get("/api/v1/me", login).statusCode()).isEqualTo(401);
    }

    @Test
    void staleRevisionIs409AndUnknownTaskIs404() throws Exception {
        start();
        var login = admin();

        assertThat(mutate("POST", "/api/v1/tasks/1/retry", login, "{\"revision\":\"6\"}")
                        .statusCode())
                .isEqualTo(409);
        assertThat(mutate("POST", "/api/v1/tasks/1/cancel", login, "{\"revision\":\"6\",\"reason\":\"x\"}")
                        .statusCode())
                .isEqualTo(409);
        assertThat(mutate("DELETE", "/api/v1/tasks/1?revision=6", login, null).statusCode())
                .isEqualTo(409);
        assertThat(mutate("POST", "/api/v1/tasks/99/retry", login, "{\"revision\":\"1\"}")
                        .statusCode())
                .isEqualTo(404);
        assertThat(mutate("DELETE", "/api/v1/tasks/99?revision=1", login, null).statusCode())
                .isEqualTo(404);
        assertThat(get("/api/v1/tasks/99", login).statusCode()).isEqualTo(404);
    }

    @Test
    void adminMutationsReachTheOperationsWithTheRevisionTheClientSent() throws Exception {
        ops.schedules.add(schedule("nightly"));
        start();
        var login = admin();

        assertThat(mutate("POST", "/api/v1/tasks/1/retry", login, "{\"revision\":\"7\"}")
                        .statusCode())
                .isEqualTo(200);
        var cancel = mutate("POST", "/api/v1/tasks/1/cancel", login, "{\"revision\":7,\"reason\":\"why\"}");
        assertThat(json(cancel)).containsEntry("outcome", "REQUESTED");
        assertThat(mutate("DELETE", "/api/v1/tasks/1?revision=7", login, null).statusCode())
                .isEqualTo(200);
        assertThat(mutate("POST", "/api/v1/queues/default/pause", login, null).statusCode())
                .isEqualTo(200);
        assertThat(mutate("POST", "/api/v1/schedules/nightly/trigger", login, null)
                        .statusCode())
                .isEqualTo(200);
        assertThat(mutate("POST", "/api/v1/schedules/missing/pause", login, null)
                        .statusCode())
                .isEqualTo(404);
        assertThat(mutate("POST", "/api/v1/queues/missing/pause", login, null).statusCode())
                .isEqualTo(404);

        assertThat(ops.calls)
                .contains(
                        "retry 1 7",
                        "cancel 1 7 why",
                        "delete 1 7",
                        "pauseQueue default true",
                        "triggerSchedule nightly",
                        "pauseSchedule missing true");
    }

    @Test
    void malformedInputIs400() throws Exception {
        start();
        var login = admin();

        assertThat(mutate("POST", "/api/v1/tasks/abc/retry", login, "{\"revision\":\"1\"}")
                        .statusCode())
                .isEqualTo(400);
        assertThat(mutate("POST", "/api/v1/tasks/1/retry", login, "{}").statusCode())
                .isEqualTo(400);
        assertThat(mutate("POST", "/api/v1/tasks/1/retry", login, "{\"revision\":\"x\"}")
                        .statusCode())
                .isEqualTo(400);
        assertThat(mutate("POST", "/api/v1/tasks/1/retry", login, "[1]").statusCode())
                .isEqualTo(400);
        assertThat(mutate("DELETE", "/api/v1/tasks/1", login, null).statusCode())
                .isEqualTo(400);
        assertThat(get("/api/v1/tasks", login).statusCode()).isEqualTo(400);
        assertThat(get("/api/v1/tasks?state=NOPE", login).statusCode()).isEqualTo(400);
        assertThat(get("/api/v1/tasks?state=DEAD&limit=0", login).statusCode()).isEqualTo(400);
        assertThat(get("/api/v1/tasks?state=DEAD&after=x", login).statusCode()).isEqualTo(400);
    }

    @Test
    void unexpectedFailureIs500WithGenericMessage() throws Exception {
        start();
        var login = admin();
        ops.failure = new IllegalStateException("secret database detail");

        var res = get("/api/v1/tasks/1", login);

        assertThat(res.statusCode()).isEqualTo(500);
        assertThat(res.body()).contains("internal error").doesNotContain("secret");
    }

    @Test
    void taskListUsesStateQueueAfterAndLimit() throws Exception {
        start();
        var login = admin();

        var all = jsonList(get("/api/v1/tasks?state=DEAD", login));
        assertThat(all).extracting(t -> t.get("id")).containsExactly("1", "2", "4");

        var page = jsonList(get("/api/v1/tasks?state=dead&limit=2", login));
        assertThat(page).extracting(t -> t.get("id")).containsExactly("1", "2");
        var next = jsonList(get("/api/v1/tasks?state=DEAD&limit=2&after=2", login));
        assertThat(next).extracting(t -> t.get("id")).containsExactly("4");

        var mail = jsonList(get("/api/v1/tasks?state=DEAD&queue=mail", login));
        assertThat(mail).extracting(t -> t.get("id")).containsExactly("2");

        get("/api/v1/tasks?state=DEAD", login);
        get("/api/v1/tasks?state=DEAD&limit=100000", login);
        assertThat(ops.calls).contains("tasks DEAD null 0 50", "tasks DEAD null 0 200", "tasks DEAD null 2 2");
    }

    @Test
    void taskJsonHasStringIdsIsoTimesAndNoPayload() throws Exception {
        start();

        var task = json(get("/api/v1/tasks/1", admin()));

        assertThat(task)
                .containsEntry("id", "1")
                .containsEntry("revision", "7")
                .containsEntry("state", "DEAD")
                .containsEntry("queue", "default")
                .containsEntry("event", "send-mail")
                .containsEntry("created", "2026-01-01T00:00:00Z")
                .containsEntry("maxRun", "PT5M")
                .containsEntry("attempts", 1L)
                .containsEntry("maxAttempts", 3L)
                .containsKeys(
                        "payloadVersion",
                        "due",
                        "stateChanged",
                        "claimedAt",
                        "leaseUntil",
                        "workerId",
                        "account",
                        "headers",
                        "uniqueKey",
                        "scheduleId",
                        "retryOf",
                        "startBy",
                        "cancelRequestedAt",
                        "cancelReason",
                        "progress",
                        "steps",
                        "failures")
                .doesNotContainKeys("payload", "data");
        assertThat(task.get("claimedAt")).isNull();
    }

    @Test
    void payloadEndpointReturnsTheEncodedPayloadOrNull() throws Exception {
        start();
        var login = viewer();

        assertThat(json(get("/api/v1/tasks/1/payload", login))).containsEntry("payload", "{\"to\":\"a\"}");
        assertThat(json(get("/api/v1/tasks/2/payload", login))).containsEntry("payload", null);
        assertThat(get("/api/v1/tasks/99/payload", login).statusCode()).isEqualTo(404);
    }

    @Test
    void overviewHasCountsPerQueueTotalsWorkersIssuesAndHealth() throws Exception {
        ops.workers.add(new WorkerInfo(1, "w", Instant.EPOCH, Instant.EPOCH, 0));
        ops.issues.add(new Issue(Instant.EPOCH, "store-error", "boom", 5L));
        ops.issues.add(new Issue(Instant.EPOCH, "lease-lost", "x", null));
        ops.health = new RunnerHealth(RunnerHealth.Status.RUNNING, 1, 2, null, null, null, null, 2);
        start();

        var overview = json(get("/api/v1/overview", viewer()));

        assertThat(overview).containsOnlyKeys("counts", "totals", "workers", "issues", "health");
        @SuppressWarnings("unchecked")
        var counts = (Map<String, Map<String, Object>>) overview.get("counts");
        assertThat(counts).containsOnlyKeys("default", "mail");
        assertThat(counts.get("default"))
                .containsOnlyKeys("QUEUED", "SCHEDULED", "LEASED", "SUCCEEDED", "DEAD", "CANCELLED", "EXPIRED")
                .containsEntry("QUEUED", 3L)
                .containsEntry("EXPIRED", 1L);
        @SuppressWarnings("unchecked")
        var totals = (Map<String, Object>) overview.get("totals");
        assertThat(totals)
                .containsEntry("QUEUED", 4L)
                .containsEntry("SUCCEEDED", 15L)
                .containsEntry("CANCELLED", 2L);
        assertThat(overview).containsEntry("workers", 1L).containsEntry("issues", 2L);
        @SuppressWarnings("unchecked")
        var health = (Map<String, Object>) overview.get("health");
        assertThat(health).containsEntry("status", "RUNNING").containsEntry("running", 2L);
    }

    @Test
    void overviewHealthIsNullAndHealthEndpointIs204WithoutARunner() throws Exception {
        start();
        var login = viewer();

        assertThat(json(get("/api/v1/overview", login))).containsEntry("health", null);
        assertThat(get("/api/v1/health", login).statusCode()).isEqualTo(204);
    }

    @Test
    void healthEndpointReturnsRunnerHealth() throws Exception {
        ops.health = new RunnerHealth(RunnerHealth.Status.QUIET, 3, 0, null, null, null, null, 0);
        start();

        var res = get("/api/v1/health", viewer());

        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(json(res)).containsEntry("status", "QUIET").containsEntry("workerId", 3L);
    }

    @Test
    void queuesSchedulesWorkersAndIssuesAreListed() throws Exception {
        ops.schedules.add(schedule("nightly"));
        ops.workers.add(new WorkerInfo(2, "w2", Instant.EPOCH, Instant.EPOCH, 1));
        ops.issues.add(new Issue(Instant.EPOCH, "store-error", "boom", 5L));
        start();
        var login = viewer();

        var queues = jsonList(get("/api/v1/queues", login));
        assertThat(queues).hasSize(1);
        assertThat(queues.get(0))
                .containsEntry("id", 1L)
                .containsEntry("name", "default")
                .containsEntry("paused", false)
                .containsKey("counts");
        var schedules = jsonList(get("/api/v1/schedules", login));
        assertThat(schedules.get(0))
                .containsEntry("id", "nightly")
                .containsEntry("fixedDelay", "PT1H")
                .containsEntry("nextFire", "2026-02-01T00:00:00Z");
        assertThat(jsonList(get("/api/v1/workers", login)).get(0))
                .containsEntry("id", 2L)
                .containsEntry("label", "w2");
        assertThat(jsonList(get("/api/v1/issues", login)).get(0))
                .containsEntry("kind", "store-error")
                .containsEntry("taskId", "5");
    }

    @Test
    void rootWithoutStaticDirIsAJsonStub() throws Exception {
        start();

        var res = get("/", null);

        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(json(res)).containsEntry("name", "bth4j admin").containsEntry("api", "/api/v1");
        assertThat(get("/other", null).statusCode()).isEqualTo(404);
    }

    @Test
    void staticFilesAreServedWithTypesNoCacheAndHtmlCspAndFallBackToIndex() throws Exception {
        var dir = Files.createDirectories(tmp.resolve("site"));
        Files.writeString(dir.resolve("index.html"), "<html>home</html>");
        Files.writeString(dir.resolve("app.js"), "console.log(1)");
        Files.writeString(dir.resolve("app.css"), "a{}");
        Files.createDirectories(dir.resolve("pages"));
        Files.writeString(dir.resolve("pages/x.js"), "x");
        start(builder().staticDir(dir));

        var index = get("/", null);
        assertThat(index.body()).isEqualTo("<html>home</html>");
        assertThat(index.headers().firstValue("Content-Type")).contains("text/html; charset=utf-8");
        assertThat(index.headers().firstValue("Cache-Control")).contains("no-cache");
        assertThat(index.headers().firstValue("Content-Security-Policy").orElseThrow())
                .contains("script-src 'self' https://cdn.jsdelivr.net", "frame-ancestors 'none'");
        assertThat(index.headers().firstValue("X-Content-Type-Options")).contains("nosniff");

        var js = get("/app.js", null);
        assertThat(js.headers().firstValue("Content-Type")).contains("text/javascript; charset=utf-8");
        assertThat(js.headers().firstValue("Content-Security-Policy")).isEmpty();
        assertThat(get("/app.css", null).headers().firstValue("Content-Type")).contains("text/css; charset=utf-8");
        assertThat(get("/pages/x.js", null).body()).isEqualTo("x");

        var fallback = get("/tasks/12", null);
        assertThat(fallback.statusCode()).isEqualTo(200);
        assertThat(fallback.body()).isEqualTo("<html>home</html>");
        assertThat(get("/missing.js", null).statusCode()).isEqualTo(404);
        assertThat(get("/api/v1/nothing", admin()).statusCode()).isEqualTo(404);
        assertThat(get("/api/unknown", null).statusCode()).isEqualTo(404);
    }

    @Test
    void pathTraversalNeverLeavesTheStaticDirectory() throws Exception {
        var dir = Files.createDirectories(tmp.resolve("site"));
        Files.writeString(dir.resolve("index.html"), "home");
        Files.writeString(tmp.resolve("secret.txt"), "TOP SECRET");
        Files.createSymbolicLink(dir.resolve("link.txt"), tmp.resolve("secret.txt"));
        start(builder().staticDir(dir));

        for (var path : List.of("/..%2fsecret.txt", "/%2e%2e/secret.txt", "/%2e%2e%2fsecret.txt", "/link.txt")) {
            var res = get(path, null);
            assertThat(res.body()).as(path).doesNotContain("TOP SECRET");
            assertThat(res.statusCode()).as(path).isEqualTo(404);
        }
        assertThat(rawGet("/../secret.txt")).doesNotContain("TOP SECRET");
        assertThat(rawGet("/a/../../secret.txt")).doesNotContain("TOP SECRET");
    }

    private String rawGet(String path) throws IOException {
        try (var socket = new Socket("127.0.0.1", server.port())) {
            socket.getOutputStream()
                    .write(("GET " + path + " HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
            return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void corsPreflightIsAnsweredForListedOriginsOnly() throws Exception {
        start(builder().corsOrigin("https://admin.example.com"));

        var ok = send(
                "OPTIONS",
                "/api/v1/tasks",
                null,
                null,
                "Origin",
                "https://admin.example.com",
                "Access-Control-Request-Method",
                "POST");
        assertThat(ok.statusCode()).isEqualTo(204);
        assertThat(ok.headers().firstValue("Access-Control-Allow-Origin")).contains("https://admin.example.com");
        assertThat(ok.headers().firstValue("Access-Control-Allow-Credentials")).contains("true");
        assertThat(ok.headers().firstValue("Vary")).contains("Origin");
        assertThat(ok.headers().firstValue("Access-Control-Allow-Methods").orElseThrow())
                .contains("GET", "POST", "DELETE");
        assertThat(ok.headers().firstValue("Access-Control-Allow-Headers").orElseThrow())
                .contains("Content-Type", "X-CSRF-Token");

        var denied = send("OPTIONS", "/api/v1/tasks", null, null, "Origin", "https://evil.example.com");
        assertThat(denied.statusCode()).isEqualTo(204);
        assertThat(denied.headers().map())
                .doesNotContainKeys(
                        "access-control-allow-origin",
                        "access-control-allow-methods",
                        "access-control-allow-credentials");
    }

    @Test
    void corsHeadersAccompanyActualResponsesForListedOriginsOnly() throws Exception {
        start(builder().corsOrigin("https://admin.example.com", "http://localhost:3000"));

        var allowed = send("GET", "/api/v1/me", null, null, "Origin", "http://localhost:3000");
        assertThat(allowed.statusCode()).isEqualTo(401);
        assertThat(allowed.headers().firstValue("Access-Control-Allow-Origin")).contains("http://localhost:3000");

        var other = send("GET", "/api/v1/me", null, null, "Origin", "https://evil.example.com");
        assertThat(other.headers().firstValue("Access-Control-Allow-Origin")).isEmpty();
        assertThat(get("/api/v1/me", null).headers().firstValue("Access-Control-Allow-Origin"))
                .isEmpty();
    }

    @Test
    void builderRequiresAdminAndAUser() {
        assertThatThrownBy(() ->
                        AdminHttpServer.builder().user("a", "b", Role.ADMIN).build())
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> AdminHttpServer.builder().admin(ops).build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void sessionsAreCappedAndEvictTheOldest() {
        var sessions = new Sessions(clock, Duration.ofHours(1), 3);
        var first = sessions.create("a", Role.ADMIN);
        clock.advance(Duration.ofSeconds(1));
        sessions.create("b", Role.ADMIN);
        clock.advance(Duration.ofSeconds(1));
        sessions.create("c", Role.ADMIN);
        clock.advance(Duration.ofSeconds(1));

        var newest = sessions.create("d", Role.ADMIN);

        assertThat(sessions.size()).isEqualTo(3);
        assertThat(sessions.find(first.token())).isNull();
        assertThat(sessions.find(newest.token())).isNotNull();
    }

    private static ScheduleView schedule(String id) {
        return new ScheduleView(
                id,
                "send-mail",
                "default",
                null,
                null,
                null,
                "SKIP",
                Duration.ofHours(1),
                true,
                false,
                Instant.parse("2026-02-01T00:00:00Z"),
                3);
    }
}
