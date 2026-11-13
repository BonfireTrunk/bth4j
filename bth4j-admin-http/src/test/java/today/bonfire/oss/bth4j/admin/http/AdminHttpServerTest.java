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
import java.security.KeyStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import today.bonfire.oss.bth4j.Issue;
import today.bonfire.oss.bth4j.Loggers;
import today.bonfire.oss.bth4j.Misfire;
import today.bonfire.oss.bth4j.Overlap;
import today.bonfire.oss.bth4j.QueueInfo;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.RunnerView;
import today.bonfire.oss.bth4j.ScheduleView;
import today.bonfire.oss.bth4j.StateCounts;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.UnservedWork;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.internal.Json;

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

    private record Login(String cookie, String csrf, String setCookie) {}

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
                .addUser("alice", "s3cret", Role.ADMIN)
                .addUser("victor", "view", Role.VIEWER)
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
                cookie.substring(0, cookie.indexOf(';')), (String) json(res).get("csrf"), cookie);
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

    private int loginFrom(String forwardedFor, String user, String password) throws Exception {
        return send(
                        "POST",
                        "/api/v1/session",
                        null,
                        Json.write(Map.of("user", user, "password", password)),
                        "X-Forwarded-For",
                        forwardedFor)
                .statusCode();
    }

    @Test
    void failedLoginsForOneUserFromManyAddressesBlockThatUserAndNoOther() throws Exception {
        start(builder().addTrustedProxy("127.0.0.1"));
        for (var i = 1; i <= 5; i++) {
            assertThat(loginFrom("203.0.113." + i, "alice", "bad")).isEqualTo(401);
        }

        assertThat(loginFrom("203.0.113.50", "alice", "s3cret")).isEqualTo(429);
        assertThat(loginFrom("203.0.113.50", "victor", "view")).isEqualTo(200);

        clock.advance(Duration.ofSeconds(11));
        assertThat(loginFrom("203.0.113.50", "alice", "s3cret")).isEqualTo(200);
    }

    @Test
    void aBlockForAUserThatDoesNotExistLooksLikeOneForAUserThatDoes() throws Exception {
        start(builder().addTrustedProxy("127.0.0.1"));
        for (var i = 1; i <= 5; i++) {
            assertThat(loginFrom("203.0.113." + i, "nobody", "bad")).isEqualTo(401);
            assertThat(loginFrom("198.51.100." + i, "alice", "bad")).isEqualTo(401);
        }

        var unknown = send(
                "POST",
                "/api/v1/session",
                null,
                Json.write(Map.of("user", "nobody", "password", "bad")),
                "X-Forwarded-For",
                "203.0.113.50");
        var known = send(
                "POST",
                "/api/v1/session",
                null,
                Json.write(Map.of("user", "alice", "password", "bad")),
                "X-Forwarded-For",
                "198.51.100.50");

        assertThat(unknown.statusCode()).isEqualTo(429);
        assertThat(known.statusCode()).isEqualTo(429);
        assertThat(unknown.body()).isEqualTo(known.body());
        assertThat(unknown.headers().firstValue("Retry-After"))
                .isEqualTo(known.headers().firstValue("Retry-After"));
    }

    @Test
    void anAttemptRefusedForItsUserIsNotCountedAgainstItsAddress() throws Exception {
        start(builder().addTrustedProxy("127.0.0.1"));
        for (var i = 1; i <= 5; i++) {
            assertThat(loginFrom("203.0.113." + i, "alice", "bad")).isEqualTo(401);
        }

        for (var i = 0; i < 5; i++) {
            assertThat(loginFrom("203.0.113.50", "alice", "s3cret")).isEqualTo(429);
        }

        assertThat(loginFrom("203.0.113.50", "victor", "view")).isEqualTo(200);
    }

    @Test
    void failedLoginsFromOneIpv6NetworkBlockTheWholeNetwork() throws Exception {
        start(builder().addTrustedProxy("127.0.0.1"));
        for (var i = 1; i <= 5; i++) {
            assertThat(loginFrom("2001:db8:1:2::" + i, "guess" + i, "bad")).isEqualTo(401);
        }

        assertThat(loginFrom("2001:db8:1:2:ffff::9", "victor", "view")).isEqualTo(429);
        assertThat(loginFrom("2001:db8:1:3::1", "victor", "view")).isEqualTo(200);
    }

    @Test
    void forwardedForIsIgnoredWhenThePeerIsNotATrustedProxy() throws Exception {
        start();
        for (var i = 1; i <= 5; i++) {
            assertThat(loginFrom("203.0.113." + i, "guess" + i, "bad")).isEqualTo(401);
        }

        assertThat(loginFrom("203.0.113.50", "victor", "view")).isEqualTo(429);
    }

    @Test
    void anOriginWithoutTextIsRefused() {
        for (String bad : new String[] {"", "  ", null}) {
            assertThatThrownBy(() -> builder().addCorsOrigin(bad))
                    .as("%s", bad)
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessage("origin is required");
        }
    }

    @Test
    void aTrustedProxyThatIsNotAnAddressOrRangeIsRefusedWithoutEchoingIt() {
        for (String bad : new String[] {"proxy.internal", "10.0.0.0/33", "10.0.0", "", "::1/129", "10.0.0.1/x", null}) {
            assertThatThrownBy(() -> builder().addTrustedProxy("10.0.0.1").addTrustedProxy(bad))
                    .as("%s", bad)
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessage("trusted proxy 2 is not an IP address or a range such as 10.0.0.0/8");
        }
    }

    @Test
    void failuresAreForgottenWhenNoAttemptFollowedForFifteenMinutes() {
        var throttle = new LoginThrottle(clock);
        for (var i = 0; i < 4; i++) assertThat(throttle.attempt("a")).isZero();
        clock.advance(Duration.ofMinutes(15).plusMillis(1));

        for (var i = 0; i < 4; i++) {
            assertThat(throttle.attempt("a"))
                    .as("attempt %d after the pause", i + 1)
                    .isZero();
        }
        clock.advance(Duration.ofMinutes(15));
        assertThat(throttle.attempt("a"))
                .as("the fifth within fifteen minutes of the fourth")
                .isZero();
        assertThat(throttle.attempt("a")).isPositive();
    }

    @Test
    void apiWithoutOrWithUnknownSessionIs401() throws Exception {
        start();

        assertThat(get("/api/v1/me", null).statusCode()).isEqualTo(401);
        assertThat(get("/api/v1/overview", new Login("bth4j_session=bogus", "x", null))
                        .statusCode())
                .isEqualTo(401);
        assertThat(send("POST", "/api/v1/queues/default/pause", null, null).statusCode())
                .isEqualTo(401);
    }

    @Test
    void meReportsUserRoleAndTheCsrfTokenOfTheSession() throws Exception {
        start();
        var login = viewer();

        assertThat(json(get("/api/v1/me", login)))
                .containsOnly(
                        Map.entry("user", "victor"), Map.entry("role", "VIEWER"), Map.entry("csrf", login.csrf()));
    }

    @Test
    void aPlusInAQueryValueIsASpace() throws Exception {
        start();

        get("/api/v1/tasks?state=DEAD&queue=bulk+mail", admin());

        assertThat(ops.calls).anyMatch(call -> call.startsWith("tasks DEAD bulk mail "));
    }

    @Test
    void anEncodedSlashCannotSpellARouteThatTakesAnId() throws Exception {
        start();
        var login = admin();

        var res = send("POST", "/api/v1/schedules%2F%7B%7D%2Ftrigger", login, null, "X-CSRF-Token", login.csrf());

        assertThat(res.statusCode()).isEqualTo(404);
        assertThat(ops.calls).noneMatch(call -> call.startsWith("triggerSchedule"));
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
                mutate("POST", "/api/v1/tasks/by-key/cancel?key=order-1", login, "{}"),
                mutate("DELETE", "/api/v1/tasks/1?revision=7", login, null),
                mutate("POST", "/api/v1/schedules/nightly/pause", login, revisionBody),
                mutate("POST", "/api/v1/schedules/nightly/resume", login, revisionBody),
                mutate("POST", "/api/v1/schedules/nightly/trigger", login, revisionBody),
                mutate("DELETE", "/api/v1/schedules/nightly?revision=5", login, null));

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
    void aSessionEndsTheTtlAfterTheSignInAlsoWhileItIsUsed() throws Exception {
        start(builder().sessionTtl(Duration.ofMinutes(10)));
        var login = admin();

        clock.advance(Duration.ofMinutes(6));
        assertThat(get("/api/v1/me", login).statusCode()).isEqualTo(200);
        clock.advance(Duration.ofMinutes(4).minusMillis(1));
        assertThat(get("/api/v1/me", login).statusCode()).isEqualTo(200);
        clock.advance(Duration.ofMillis(1));

        assertThat(get("/api/v1/me", login).statusCode()).isEqualTo(401);
    }

    @Test
    void anExpiredSessionCannotMutateWithItsFormerCsrfToken() throws Exception {
        start(builder().sessionTtl(Duration.ofMinutes(10)));
        var login = admin();
        clock.advance(Duration.ofMinutes(11));

        var responses = List.of(
                mutate("POST", "/api/v1/tasks/1/retry", login, "{\"revision\":\"7\"}"),
                mutate("POST", "/api/v1/queues/default/pause", login, null),
                mutate("DELETE", "/api/v1/tasks/1?revision=7", login, null));

        assertThat(responses).extracting(HttpResponse::statusCode).containsOnly(401);
        assertThat(ops.calls).isEmpty();
    }

    @Test
    void aLoggedOutSessionCannotMutateWithItsFormerCsrfToken() throws Exception {
        start();
        var login = admin();
        assertThat(mutate("DELETE", "/api/v1/session", login, null).statusCode())
                .isEqualTo(200);

        var responses = List.of(
                mutate("POST", "/api/v1/tasks/1/retry", login, "{\"revision\":\"7\"}"),
                mutate("POST", "/api/v1/queues/default/pause", login, null),
                mutate("DELETE", "/api/v1/tasks/1?revision=7", login, null));

        assertThat(responses).extracting(HttpResponse::statusCode).containsOnly(401);
        assertThat(ops.calls).isEmpty();
    }

    @Test
    void cookiesAreSecureWhenTheServerSpeaksHttps(@TempDir Path keys) throws Exception {
        var keystore = keys.resolve("server.p12");
        var keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        var process = new ProcessBuilder(
                        keytool,
                        "-genkeypair",
                        "-alias",
                        "admin",
                        "-keyalg",
                        "RSA",
                        "-keysize",
                        "2048",
                        "-dname",
                        "CN=127.0.0.1",
                        "-ext",
                        "san=ip:127.0.0.1",
                        "-validity",
                        "2",
                        "-storetype",
                        "PKCS12",
                        "-keystore",
                        keystore.toString(),
                        "-storepass",
                        "changeit")
                .redirectErrorStream(true)
                .start();
        var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(process.waitFor()).as(output).isZero();
        var store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(keystore)) {
            store.load(in, "changeit".toCharArray());
        }
        var keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(store, "changeit".toCharArray());
        var serverSsl = SSLContext.getInstance("TLS");
        serverSsl.init(keyManagers.getKeyManagers(), null, null);
        var trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(store);
        var clientSsl = SSLContext.getInstance("TLS");
        clientSsl.init(null, trust.getTrustManagers(), null);
        start(builder().https(serverSsl));
        var https = HttpClient.newBuilder().sslContext(clientSsl).build();
        var base = "https://127.0.0.1:" + server.port() + "/api/v1/session";

        var login = https.send(
                HttpRequest.newBuilder(URI.create(base))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                Json.write(Map.of("user", "alice", "password", "s3cret"))))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(login.statusCode()).isEqualTo(200);
        var cookie = login.headers().firstValue("Set-Cookie").orElseThrow();
        assertThat(cookie).contains("HttpOnly", "SameSite=Strict", "Path=/", "Secure");
        var logout = https.send(
                HttpRequest.newBuilder(URI.create(base))
                        .method("DELETE", HttpRequest.BodyPublishers.noBody())
                        .header("Cookie", cookie.substring(0, cookie.indexOf(';')))
                        .header("X-CSRF-Token", (String) json(login).get("csrf"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(logout.statusCode()).isEqualTo(200);
        assertThat(logout.headers().firstValue("Set-Cookie").orElseThrow()).contains("Max-Age=0", "Secure");
    }

    @Test
    void taskIdsAbove2Pow53RoundTripExactlyThroughReadsAndMutations() throws Exception {
        var big = (1L << 53) + 1;
        var max = Long.MAX_VALUE;
        ops.tasks.put(big, FakeAdminOps.task(big, 7, TaskState.DEAD, "default", "p"));
        ops.tasks.put(max, FakeAdminOps.task(max, 7, TaskState.DEAD, "default", "q"));
        var waiting = max - 1;
        ops.tasks.put(waiting, FakeAdminOps.task(waiting, 7, TaskState.QUEUED, "default", "w"));
        start();
        var login = admin();

        assertThat(json(get("/api/v1/tasks/" + big, login))).containsEntry("id", String.valueOf(big));
        assertThat(json(get("/api/v1/tasks/" + max, login))).containsEntry("id", String.valueOf(max));
        var page = json(get("/api/v1/tasks?state=DEAD&limit=1&cursor=4", login));
        assertThat(items(page)).extracting(t -> t.get("id")).containsExactly(String.valueOf(big));
        assertThat(page).containsEntry("next", String.valueOf(big));
        var next = json(get("/api/v1/tasks?state=DEAD&limit=1&cursor=" + page.get("next"), login));
        assertThat(items(next)).extracting(t -> t.get("id")).containsExactly(String.valueOf(max));
        assertThat(mutate("POST", "/api/v1/tasks/" + big + "/retry", login, "{\"revision\":\"7\"}")
                        .statusCode())
                .isEqualTo(200);
        assertThat(mutate("POST", "/api/v1/tasks/" + waiting + "/cancel", login, "{\"revision\":7,\"reason\":\"r\"}")
                        .statusCode())
                .isEqualTo(200);
        assertThat(mutate("DELETE", "/api/v1/tasks/" + big + "?revision=7", login, null)
                        .statusCode())
                .isEqualTo(200);

        assertThat(ops.calls).contains("retry " + big + " 7", "cancel " + waiting + " 7 r", "delete " + big + " 7");
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
    void taskByKeyIsReadableByViewersAndDecodesTheKey() throws Exception {
        ops.keyHolders.put("order 1/a+b", 1L);
        ops.keyHolders.put("..", 3L);
        ops.keyHolders.put(" padded ", 3L);
        start();
        var login = viewer();

        var found = get("/api/v1/tasks/by-key?key=order%201%2Fa%2Bb", login);

        assertThat(found.statusCode()).isEqualTo(200);
        assertThat(json(found)).containsEntry("id", "1");
        assertThat(json(get("/api/v1/tasks/by-key?key=..", login))).containsEntry("id", "3");
        assertThat(json(get("/api/v1/tasks/by-key?key=%20padded%20", login))).containsEntry("id", "3");
        assertThat(get("/api/v1/tasks/by-key", login).statusCode()).isEqualTo(400);
        assertThat(get("/api/v1/tasks/by-key?key=missing", login).statusCode()).isEqualTo(404);
    }

    @Test
    void cancelByKeyReachesTheOperations() throws Exception {
        ops.keyHolders.put("order-1", 3L);
        start();
        var login = admin();

        var cancel = mutate("POST", "/api/v1/tasks/by-key/cancel?key=order-1", login, "{\"reason\":\"why\"}");

        assertThat(cancel.statusCode()).isEqualTo(200);
        assertThat(json(cancel)).containsEntry("outcome", "REQUESTED");
        assertThat(ops.calls).containsExactly("cancelByKey order-1 why");
        assertThat(mutate("POST", "/api/v1/tasks/by-key/cancel?key=order-1", login, "{\"reason\":5}")
                        .statusCode())
                .isEqualTo(400);
        assertThat(get("/api/v1/tasks/by-key/cancel?key=order-1", login).statusCode())
                .isEqualTo(404);
        assertThat(mutate("POST", "/api/v1/tasks/by-key/order-1/cancel", login, "{}")
                        .statusCode())
                .isEqualTo(404);
    }

    @Test
    void cancelByKeyAnswersEachOutcomeWithItsOwnStatusAndText() throws Exception {
        ops.keyHolders.put("done", 1L);
        ops.keyHolders.put("racy", 3L);
        start();
        var login = admin();

        var missing = mutate("POST", "/api/v1/tasks/by-key/cancel?key=missing", login, "{}");
        var finished = mutate("POST", "/api/v1/tasks/by-key/cancel?key=done", login, "{}");
        ops.staleKeys.add("racy");
        var stale = mutate("POST", "/api/v1/tasks/by-key/cancel?key=racy", login, "{}");

        assertRefusal(missing, 404, "NOT_FOUND", "no task holds this key");
        assertRefusal(finished, 409, "WRONG_STATE", "the task holding this key is already finished");
        assertRefusal(stale, 409, "STALE", "the task changed since it was loaded");
    }

    @Test
    void cancelByKeyNeedsTheCsrfTokenAndANumericKeyIsAKey() throws Exception {
        ops.keyHolders.put("42", 3L);
        start();
        var login = admin();

        var noToken = send("POST", "/api/v1/tasks/by-key/cancel?key=42", login, "{}");

        assertThat(noToken.statusCode()).isEqualTo(403);
        assertThat(get("/api/v1/tasks/by-key?key=42", login).statusCode()).isEqualTo(200);
        assertThat(mutate("POST", "/api/v1/tasks/by-key/cancel?key=42", login, "{}")
                        .statusCode())
                .isEqualTo(200);
        assertThat(ops.calls).containsExactly("cancelByKey 42 null");
    }

    @Test
    void byKeyRoutesRejectAnInvalidKey() throws Exception {
        start();
        var login = admin();
        var tooLong = "k".repeat(257);

        assertThat(get("/api/v1/tasks/by-key?key=" + tooLong, login).statusCode())
                .isEqualTo(400);
        assertThat(get("/api/v1/tasks/by-key?key=%20", login).statusCode()).isEqualTo(400);
        assertThat(mutate("POST", "/api/v1/tasks/by-key/cancel?key=" + tooLong, login, "{}")
                        .statusCode())
                .isEqualTo(400);
    }

    @Test
    void aRetryThatIsRefusedAnswersTheReasonWithAStableCode() throws Exception {
        start();
        var login = admin();

        var missing = mutate("POST", "/api/v1/tasks/99/retry", login, "{\"revision\":\"1\"}");
        var stale = mutate("POST", "/api/v1/tasks/1/retry", login, "{\"revision\":\"6\"}");
        var queued = mutate("POST", "/api/v1/tasks/3/retry", login, "{\"revision\":\"1\"}");

        assertRefusal(missing, 404, "NOT_FOUND", "there is no such task");
        assertRefusal(stale, 409, "STALE", "the task changed since it was loaded");
        assertRefusal(queued, 409, "WRONG_STATE", "only a dead, cancelled or expired task can be retried");
        assertThat(ops.calls).containsExactly("retry 99 1", "retry 1 6", "retry 3 1");
    }

    @Test
    void aCancelThatIsRefusedAnswersTheReasonWithAStableCode() throws Exception {
        start();
        var login = admin();

        var missing = mutate("POST", "/api/v1/tasks/99/cancel", login, "{\"revision\":1}");
        var stale = mutate("POST", "/api/v1/tasks/1/cancel", login, "{\"revision\":6}");
        var finished = mutate("POST", "/api/v1/tasks/1/cancel", login, "{\"revision\":7}");

        assertRefusal(missing, 404, "NOT_FOUND", "there is no such task");
        assertRefusal(stale, 409, "STALE", "the task changed since it was loaded");
        assertRefusal(finished, 409, "WRONG_STATE", "the task has finished and cannot be cancelled");
    }

    @Test
    void aDeleteThatIsRefusedAnswersTheReasonWithAStableCode() throws Exception {
        ops.tasks.put(5L, FakeAdminOps.task(5, 1, TaskState.LEASED, "default", "z"));
        start();
        var login = admin();

        var missing = mutate("DELETE", "/api/v1/tasks/99?revision=1", login, null);
        var stale = mutate("DELETE", "/api/v1/tasks/5?revision=2", login, null);
        var leased = mutate("DELETE", "/api/v1/tasks/5?revision=1", login, null);

        assertRefusal(missing, 404, "NOT_FOUND", "there is no such task");
        assertRefusal(stale, 409, "STALE", "the task changed since it was loaded");
        assertRefusal(leased, 409, "WRONG_STATE", "a task that is running cannot be deleted");
        assertThat(ops.calls).containsExactly("delete 99 1", "delete 5 2", "delete 5 1");
    }

    @Test
    void everyScheduleActionAnswers200404Or409WithTheRevisionItWasSent() throws Exception {
        ops.schedules.add(schedule("nightly"));
        start();
        var login = admin();
        var applied = "{\"revision\":\"5\"}";
        var stale = "{\"revision\":\"4\"}";

        var actions = List.of("pause", "resume", "trigger");
        for (var action : actions) {
            var path = "/api/v1/schedules/nightly/" + action;
            assertThat(mutate("POST", path, login, applied).statusCode())
                    .as(action)
                    .isEqualTo(200);
            assertRefusal(mutate("POST", path, login, stale), 409, "STALE", "the schedule changed since it was loaded");
            assertRefusal(
                    mutate("POST", "/api/v1/schedules/missing/" + action, login, applied),
                    404,
                    "NOT_FOUND",
                    "there is no such schedule");
        }
        assertRefusal(
                mutate("DELETE", "/api/v1/schedules/nightly?revision=4", login, null),
                409,
                "STALE",
                "the schedule changed since it was loaded");
        assertRefusal(
                mutate("DELETE", "/api/v1/schedules/missing?revision=5", login, null),
                404,
                "NOT_FOUND",
                "there is no such schedule");
        assertThat(mutate("DELETE", "/api/v1/schedules/nightly?revision=5", login, null)
                        .statusCode())
                .isEqualTo(200);
        assertThat(ops.calls)
                .contains(
                        "pauseSchedule nightly 5 true",
                        "pauseSchedule nightly 5 false",
                        "triggerSchedule nightly 4",
                        "deleteSchedule nightly 4",
                        "deleteSchedule nightly 5");
    }

    @Test
    void aScheduleActionWithoutAUsableRevisionIs400AndReachesNoOperation() throws Exception {
        ops.schedules.add(schedule("nightly"));
        start();
        var login = admin();

        for (var action : List.of("pause", "resume", "trigger")) {
            var path = "/api/v1/schedules/nightly/" + action;
            assertThat(mutate("POST", path, login, "{}").statusCode())
                    .as(action)
                    .isEqualTo(400);
            assertThat(mutate("POST", path, login, "{\"revision\":\"x\"}").statusCode())
                    .as(action)
                    .isEqualTo(400);
        }
        assertThat(mutate("DELETE", "/api/v1/schedules/nightly", login, null).statusCode())
                .isEqualTo(400);
        assertThat(mutate("DELETE", "/api/v1/schedules/nightly?revision=x", login, null)
                        .statusCode())
                .isEqualTo(400);
        assertThat(ops.calls).isEmpty();
    }

    @Test
    void aScheduleRevisionBeyond32BitsIsSentToTheOperationUnchanged() throws Exception {
        ops.schedules.add(schedule("nightly"));
        start();
        var login = admin();

        assertThat(mutate("POST", "/api/v1/schedules/nightly/trigger", login, "{\"revision\":\"5000000000\"}")
                        .statusCode())
                .isEqualTo(409);
        assertThat(ops.calls).containsExactly("triggerSchedule nightly 5000000000");
    }

    @Test
    void aRefusedActionDoesNotLookTheTaskUpAgain() throws Exception {
        start();
        var login = admin();

        mutate("POST", "/api/v1/tasks/3/retry", login, "{\"revision\":\"1\"}");
        mutate("DELETE", "/api/v1/tasks/99?revision=1", login, null);

        assertThat(ops.taskLookups).isZero();
    }

    private static void assertRefusal(HttpResponse<String> res, int status, String code, String message) {
        assertThat(res.statusCode()).isEqualTo(status);
        assertThat(json(res)).containsExactlyInAnyOrderEntriesOf(Map.of("error", message, "code", code));
    }

    @Test
    void adminMutationsReachTheOperationsWithTheRevisionTheClientSent() throws Exception {
        ops.schedules.add(schedule("nightly"));
        start();
        var login = admin();

        assertThat(mutate("POST", "/api/v1/tasks/1/retry", login, "{\"revision\":\"7\"}")
                        .statusCode())
                .isEqualTo(200);
        var cancel = mutate("POST", "/api/v1/tasks/3/cancel", login, "{\"revision\":1,\"reason\":\"why\"}");
        assertThat(json(cancel)).containsEntry("outcome", "REQUESTED");
        assertThat(mutate("DELETE", "/api/v1/tasks/1?revision=7", login, null).statusCode())
                .isEqualTo(200);
        assertThat(mutate("POST", "/api/v1/queues/default/pause", login, null).statusCode())
                .isEqualTo(200);
        assertThat(mutate("POST", "/api/v1/schedules/nightly/trigger", login, "{\"revision\":\"5\"}")
                        .statusCode())
                .isEqualTo(200);
        assertThat(mutate("POST", "/api/v1/schedules/missing/pause", login, "{\"revision\":\"5\"}")
                        .statusCode())
                .isEqualTo(404);
        assertThat(mutate("POST", "/api/v1/queues/missing/pause", login, null).statusCode())
                .isEqualTo(404);

        assertThat(ops.calls)
                .contains(
                        "retry 1 7",
                        "cancel 3 1 why",
                        "delete 1 7",
                        "pauseQueue default true",
                        "triggerSchedule nightly 5",
                        "pauseSchedule missing 5 true");
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
    void anUnreadableTaskIs422WithItsCodeAndNoStoredDetail() throws Exception {
        start();
        var login = admin();
        ops.failure = new today.bonfire.oss.bth4j.exceptions.UnreadableTaskException(
                1, "task 1 has a stored value that cannot be read", new IllegalArgumentException("secret value"));

        var res = get("/api/v1/tasks/1", login);

        assertThat(res.statusCode()).isEqualTo(422);
        assertThat(res.body()).contains("UNREADABLE_TASK").doesNotContain("secret");
        assertThat(get("/api/v1/tasks/by-key?key=welcome", login).statusCode()).isEqualTo(422);
    }

    @Test
    void taskListUsesStateQueueAndLimit() throws Exception {
        start();
        var login = admin();

        var all = items(json(get("/api/v1/tasks?state=DEAD", login)));
        assertThat(all).extracting(t -> t.get("id")).containsExactly("1", "2", "4");

        var page = items(json(get("/api/v1/tasks?state=dead&limit=2", login)));
        assertThat(page).extracting(t -> t.get("id")).containsExactly("1", "2");
        var next = items(json(get("/api/v1/tasks?state=DEAD&limit=2&cursor=2", login)));
        assertThat(next).extracting(t -> t.get("id")).containsExactly("4");

        var mail = items(json(get("/api/v1/tasks?state=DEAD&queue=mail", login)));
        assertThat(mail).extracting(t -> t.get("id")).containsExactly("2");
        var emptyQueue = items(json(get("/api/v1/tasks?state=DEAD&queue=", login)));
        assertThat(emptyQueue).extracting(t -> t.get("id")).containsExactly("1", "2", "4");

        get("/api/v1/tasks?state=DEAD", login);
        get("/api/v1/tasks?state=DEAD&limit=100000", login);
        assertThat(ops.calls)
                .contains(
                        "tasks DEAD null cursor=null 50",
                        "tasks DEAD null cursor=null 200",
                        "tasks DEAD null cursor=2 2");
    }

    @Test
    void taskListWithCursorReturnsItemsAndNextAndWalks() throws Exception {
        start();
        var login = admin();

        var first = json(get("/api/v1/tasks?state=DEAD&limit=2&cursor=", login));
        assertThat(items(first)).extracting(t -> t.get("id")).containsExactly("1", "2");
        assertThat(first).containsEntry("next", "2");

        var second = json(get("/api/v1/tasks?state=DEAD&limit=2&cursor=2", login));
        assertThat(items(second)).extracting(t -> t.get("id")).containsExactly("4");
        assertThat(second).containsKey("next").containsEntry("next", null);

        var blank = json(get("/api/v1/tasks?state=DEAD&limit=50&cursor=", login));
        assertThat(items(blank)).hasSize(3);

        assertThat(items(json(get("/api/v1/tasks?state=DEAD", login)))).hasSize(3);
        assertThat(ops.calls).contains("tasks DEAD null cursor=null 2", "tasks DEAD null cursor=2 2");
        assertThat(get("/api/v1/tasks?state=DEAD&cursor=x", login).statusCode()).isEqualTo(400);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> items(Map<String, Object> page) {
        return (List<Map<String, Object>>) page.get("items");
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
                .containsEntry("type", "send-mail")
                .containsEntry("createdAt", "2026-01-01T00:00:00Z")
                .containsEntry("maxRunTime", "PT5M")
                .containsEntry("attempts", 1L)
                .containsEntry("maxAttempts", 3L)
                .containsKeys(
                        "payloadVersion",
                        "dueAt",
                        "stateChangedAt",
                        "claimedAt",
                        "leaseUntil",
                        "runnerId",
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
    void overviewHasCountsPerQueueTotalsRunnersIssuesAndHealth() throws Exception {
        ops.runners.add(new RunnerView(1, "w", Instant.EPOCH, Instant.EPOCH, 0, true, Set.of(), Set.of()));
        ops.issues.add(new Issue(Instant.EPOCH, Issue.Kind.STORE_ERROR, "boom", 5L));
        ops.issues.add(new Issue(Instant.EPOCH, Issue.Kind.LEASE_LOST, "x", null));
        ops.health = new RunnerHealth(RunnerHealth.Status.RUNNING, 1, 2, 0, null, null, null, null, 2);
        start();

        var overview = json(get("/api/v1/overview", viewer()));

        assertThat(overview).containsOnlyKeys("counts", "totals", "runners", "issues", "health");
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
        assertThat(overview).containsEntry("runners", 1L).containsEntry("issues", 2L);
        @SuppressWarnings("unchecked")
        var health = (Map<String, Object>) overview.get("health");
        assertThat(health).containsEntry("status", "RUNNING").containsEntry("runningTasks", 2L);
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
        ops.health = new RunnerHealth(RunnerHealth.Status.QUIET, 3, 0, 0, null, null, null, null, 0);
        start();

        var res = get("/api/v1/health", viewer());

        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(json(res)).containsEntry("status", "QUIET").containsEntry("runnerId", 3L);
    }

    @Test
    void queuesSchedulesRunnersAndIssuesAreListed() throws Exception {
        ops.schedules.add(schedule("nightly"));
        ops.runners.add(new RunnerView(2, "w2", Instant.EPOCH, Instant.EPOCH, 1, true, Set.of(), Set.of()));
        ops.issues.add(new Issue(Instant.EPOCH, Issue.Kind.STORE_ERROR, "boom", 5L));
        start();
        var login = viewer();

        var queues = jsonList(get("/api/v1/queues", login));
        assertThat(queues).hasSize(1);
        assertThat(queues.get(0))
                .containsEntry("id", 1L)
                .containsEntry("name", "default")
                .containsEntry("paused", false)
                .containsKey("counts");
        var schedules = items(json(get("/api/v1/schedules", login)));
        assertThat(schedules.get(0))
                .containsEntry("id", "nightly")
                .containsEntry("revision", "5")
                .containsEntry("fixedDelay", "PT1H")
                .containsEntry("misfire", "SKIP")
                .containsEntry("overlap", "SKIP")
                .doesNotContainKey("catchUpMaxFires")
                .containsEntry("maxRunTime", "PT10M")
                .containsEntry("nextFireAt", "2026-02-01T00:00:00Z");
        assertThat(jsonList(get("/api/v1/runners", login)).get(0))
                .containsEntry("id", 2L)
                .containsEntry("label", "w2");
        assertThat(jsonList(get("/api/v1/issues", login)).get(0))
                .containsEntry("kind", "store-error")
                .containsEntry("taskId", "5");
    }

    private static ScheduleView schedule(String id, Instant nextFireAt, boolean paused) {
        var base = schedule(id);
        return new ScheduleView(
                base.id(),
                base.revision(),
                base.typeName(),
                base.queue(),
                base.account(),
                base.cron(),
                base.zone(),
                base.misfire(),
                base.fixedDelay(),
                base.overlap(),
                paused,
                nextFireAt,
                base.maxAttempts(),
                base.maxRunTime());
    }

    @Test
    void scheduleListPagesByCursorWithTheShapeOfTheTaskList() throws Exception {
        for (var id : List.of("a", "b", "c")) ops.schedules.add(schedule(id));
        start();
        var login = viewer();

        var first = json(get("/api/v1/schedules?limit=2", login));
        assertThat(items(first)).extracting(t -> t.get("id")).containsExactly("a", "b");
        assertThat(first).containsEntry("next", "b");
        var second = json(get("/api/v1/schedules?limit=2&cursor=b", login));
        assertThat(items(second)).extracting(t -> t.get("id")).containsExactly("c");
        assertThat(second).containsKey("next").containsEntry("next", null);
        assertThat(items(json(get("/api/v1/schedules?cursor=", login)))).hasSize(3);

        get("/api/v1/schedules", login);
        get("/api/v1/schedules?limit=100000", login);
        assertThat(ops.calls)
                .contains(
                        "schedules cursor=null 2",
                        "schedules cursor=b 2",
                        "schedules cursor=null 100",
                        "schedules cursor=null 200");
    }

    @Test
    void scheduleListRefusesABadLimitAsTheTaskListDoes() throws Exception {
        start();
        var login = viewer();

        for (var bad : List.of("0", "-1", "x", "1.5")) {
            var res = get("/api/v1/schedules?limit=" + bad, login);
            var task = get("/api/v1/tasks?state=DEAD&limit=" + bad, login);
            assertThat(res.statusCode()).as("limit=%s", bad).isEqualTo(400).isEqualTo(task.statusCode());
            assertThat(res.body()).as("limit=%s", bad).isEqualTo(task.body());
            assertThat(get("/api/v1/schedules/upcoming?limit=" + bad, login).statusCode())
                    .as("upcoming limit=%s", bad)
                    .isEqualTo(400);
        }
        assertThat(ops.calls).noneMatch(c -> c.startsWith("schedules ") || c.startsWith("upcomingSchedules"));
    }

    @Test
    void upcomingSchedulesAreAListOfTheSoonestUnpausedOnesWithADefaultOfEight() throws Exception {
        var base = Instant.parse("2026-02-01T00:00:00Z");
        for (int i = 0; i < 10; i++) ops.schedules.add(schedule("s" + i, base.plusSeconds(100 - i), false));
        ops.schedules.add(schedule("paused", base, true));
        start();
        var login = viewer();

        var res = get("/api/v1/schedules/upcoming", login);
        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(jsonList(res))
                .extracting(t -> t.get("id"))
                .containsExactly("s9", "s8", "s7", "s6", "s5", "s4", "s3", "s2");
        assertThat(jsonList(get("/api/v1/schedules/upcoming?limit=2", login)))
                .extracting(t -> t.get("id"))
                .containsExactly("s9", "s8");
        get("/api/v1/schedules/upcoming?limit=100000", login);
        assertThat(ops.calls).contains("upcomingSchedules 8", "upcomingSchedules 2", "upcomingSchedules 200");
    }

    @Test
    void upcomingIsARouteOfItsOwnAndNotAScheduleId() throws Exception {
        ops.schedules.add(schedule("upcoming"));
        start();
        var login = admin();

        assertThat(get("/api/v1/schedules/other", login).statusCode()).isEqualTo(404);
        assertThat(get("/api/v1/schedules/upcoming/pause", login).statusCode()).isEqualTo(404);
        assertThat(jsonList(get("/api/v1/schedules/upcoming", login))).hasSize(1);
        assertThat(mutate("POST", "/api/v1/schedules/upcoming/pause", login, "{\"revision\":\"5\"}")
                        .statusCode())
                .isEqualTo(200);
        assertThat(ops.calls).contains("pauseSchedule upcoming 5 true");
    }

    @Test
    void scheduleReadsNeedASessionAndAViewerMayMakeThem() throws Exception {
        start();

        assertThat(get("/api/v1/schedules", null).statusCode()).isEqualTo(401);
        assertThat(get("/api/v1/schedules/upcoming", null).statusCode()).isEqualTo(401);
        assertThat(get("/api/v1/schedules", viewer()).statusCode()).isEqualTo(200);
        assertThat(get("/api/v1/schedules/upcoming", viewer()).statusCode()).isEqualTo(200);
    }

    @Test
    void unservedEndpointListsQueueAndTypeReportsForViewers() throws Exception {
        ops.unserved.add(new UnservedWork("mail", null, 12));
        ops.unserved.add(new UnservedWork("default", "export", 3));
        start();

        var response = get("/api/v1/unserved", viewer());
        assertThat(response.statusCode()).isEqualTo(200);
        var reports = jsonList(response);
        assertThat(reports).hasSize(2);
        assertThat(reports.getFirst())
                .containsOnlyKeys("queue", "type", "waiting")
                .containsEntry("queue", "mail")
                .containsEntry("type", null)
                .containsEntry("waiting", 12L);
        assertThat(reports.get(1))
                .containsEntry("queue", "default")
                .containsEntry("type", "export")
                .containsEntry("waiting", 3L);
        assertThat(ops.calls).contains("unservedWork PT1M");
        ops.unserved.clear();
        assertThat(jsonList(get("/api/v1/unserved", viewer()))).isEmpty();
        assertThat(get("/api/v1/unserved", null).statusCode()).isEqualTo(401);
    }

    @Test
    void runnersViewIncludesCoverageNamesAndEmptyCoverage() throws Exception {
        ops.runners.add(new RunnerView(
                1,
                "known",
                Instant.EPOCH,
                Instant.parse("2026-02-01T00:00:00Z"),
                2,
                true,
                Set.of("mail", "default"),
                Set.of("send-mail", "export")));
        ops.runners.add(new RunnerView(3, "empty", Instant.EPOCH, Instant.EPOCH, 0, false, Set.of(), Set.of()));
        start();

        var runners = jsonList(get("/api/v1/runners", viewer()));
        assertThat(runners).hasSize(2);
        assertThat(runners.getFirst())
                .containsOnlyKeys("id", "label", "startedAt", "lastSeenAt", "running", "accepting", "queues", "types")
                .containsEntry("accepting", true)
                .containsEntry("lastSeenAt", "2026-02-01T00:00:00Z")
                .containsEntry("queues", List.of("default", "mail"))
                .containsEntry("types", List.of("export", "send-mail"));
        assertThat(runners.get(1))
                .containsEntry("accepting", false)
                .containsEntry("queues", List.of())
                .containsEntry("types", List.of());
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
                .contains("script-src 'self' 'unsafe-eval';", "frame-ancestors 'none'")
                .doesNotContain("jsdelivr");
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
    void aStaticFileCarriesAnEtagAndAMatchingIfNoneMatchIsAnsweredWithoutABody() throws Exception {
        var dir = Files.createDirectories(tmp.resolve("site"));
        Files.writeString(dir.resolve("index.html"), "<html>home</html>");
        Files.writeString(dir.resolve("app.js"), "console.log(1)");
        start(builder().staticDir(dir));

        var js = get("/app.js", null);
        var etag = js.headers().firstValue("ETag").orElseThrow();
        assertThat(etag).matches("\"[0-9a-f]{32}\"");
        assertThat(get("/app.js", null).headers().firstValue("ETag")).contains(etag);

        for (var header : List.of(etag, "W/" + etag, "\"other\", " + etag, "*")) {
            var res = send("GET", "/app.js", null, null, "If-None-Match", header);
            assertThat(res.statusCode()).as(header).isEqualTo(304);
            assertThat(res.body()).as(header).isEmpty();
            assertThat(res.headers().firstValue("ETag")).as(header).contains(etag);
            assertThat(res.headers().firstValue("Cache-Control")).as(header).contains("no-cache");
        }
        assertThat(send("GET", "/app.js", null, null, "If-None-Match", "\"other\"")
                        .statusCode())
                .isEqualTo(200);

        var index = get("/", null);
        var html = send(
                "GET",
                "/",
                null,
                null,
                "If-None-Match",
                index.headers().firstValue("ETag").orElseThrow());
        assertThat(html.statusCode()).isEqualTo(304);
        assertThat(html.headers().firstValue("Content-Security-Policy"))
                .isEqualTo(index.headers().firstValue("Content-Security-Policy"));
    }

    @Test
    void theEtagOfAStaticFileFollowsItsBytesNotItsSizeOrModificationTime() throws Exception {
        var dir = Files.createDirectories(tmp.resolve("site"));
        var file = dir.resolve("app.js");
        var old = java.nio.file.attribute.FileTime.fromMillis(1_000_000L);
        Files.writeString(file, "version=1");
        Files.setLastModifiedTime(file, old);
        start(builder().staticDir(dir));
        var first = get("/app.js", null);
        var oldTag = first.headers().firstValue("ETag").orElseThrow();

        var unchanged = send("GET", "/app.js", null, null, "If-None-Match", oldTag);
        assertThat(unchanged.statusCode()).isEqualTo(304);

        Files.writeString(file, "version=2");
        Files.setLastModifiedTime(file, old);
        var replaced = send("GET", "/app.js", null, null, "If-None-Match", oldTag);

        assertThat(replaced.statusCode()).isEqualTo(200);
        assertThat(replaced.body()).isEqualTo("version=2");
        assertThat(replaced.headers().firstValue("ETag").orElseThrow()).isNotEqualTo(oldTag);
    }

    @Test
    void apiAnswersCarryADateHeaderAndAnAllowedOriginMayReadIt() throws Exception {
        start(builder().addCorsOrigin("https://admin.example.com"));

        var plain = get("/api/v1/me", null);
        assertThat(plain.headers().firstValue("Date")).isPresent();
        assertThat(plain.headers().firstValue("Access-Control-Expose-Headers")).isEmpty();

        var allowed = send("GET", "/api/v1/me", null, null, "Origin", "https://admin.example.com");
        assertThat(allowed.headers().firstValue("Date")).isPresent();
        assertThat(allowed.headers().firstValue("Access-Control-Expose-Headers"))
                .contains("Date");

        var denied = send("GET", "/api/v1/me", null, null, "Origin", "https://evil.example.com");
        assertThat(denied.headers().firstValue("Access-Control-Expose-Headers")).isEmpty();
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
        start(builder().addCorsOrigin("https://admin.example.com"));

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
        start(builder().addCorsOrigin("https://admin.example.com").addCorsOrigin("http://localhost:3000"));

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
                        AdminHttpServer.builder().addUser("a", "b", Role.ADMIN).build())
                .isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> AdminHttpServer.builder().admin(ops).build())
                .isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void aSessionTtlThatIsNotPositiveIsRefused() {
        assertThatThrownBy(() -> builder().sessionTtl(Duration.ZERO).build())
                .isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void aSessionTtlBelowOneSecondIsRefusedBecauseItsCookieWouldBeDeletedAtOnce() {
        assertThatThrownBy(() -> builder().sessionTtl(Duration.ofMillis(999)).build())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageContaining("at least one second");
        assertThat(builder().sessionTtl(Duration.ofSeconds(1)).build()).isNotNull();
    }

    @Test
    void aSecondUserWithTheSameNameIsRefusedSoThatNeitherPasswordIsDroppedUnseen() {
        var builder = AdminHttpServer.builder().addUser("alice", "first", Role.ADMIN);

        assertThatThrownBy(() -> builder.addUser("alice", "second", Role.VIEWER))
                .isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void aUserWithAnEmptyPasswordIsRefused() {
        assertThatThrownBy(() -> AdminHttpServer.builder().addUser("a", "", Role.ADMIN))
                .isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void anUnknownQueueIsABadRequestOnTheTaskListAndNotFoundOnPause() throws Exception {
        ops.queueFailure = new today.bonfire.oss.bth4j.exceptions.TaskValidationException("Unknown queue: nope");
        start();
        var login = admin();

        var list = get("/api/v1/tasks?state=DEAD&queue=nope", login);
        assertThat(list.statusCode()).isEqualTo(400);
        assertThat(json(list)).containsEntry("error", "Unknown queue: nope");
        assertThat(mutate("POST", "/api/v1/queues/default/pause", login, null).statusCode())
                .isEqualTo(404);
    }

    @Test
    void secureCookieFollowsTheBuilderSettingIndependentlyOfHttps() throws Exception {
        start(builder().secureCookie(true));

        var res = rawLogin("alice", "s3cret");

        assertThat(res.headers().firstValue("Set-Cookie").orElseThrow()).contains("Secure");
    }

    @Test
    void onlyTheSignInSetsTheSessionCookieAndItsMaxAgeIsTheTtl() throws Exception {
        start(builder().sessionTtl(Duration.ofMinutes(10)));
        var login = admin();
        clock.advance(Duration.ofMinutes(6));

        assertThat(login.setCookie()).startsWith(login.cookie() + ";").contains("Max-Age=600");
        assertThat(get("/api/v1/me", login).headers().firstValue("Set-Cookie")).isEmpty();
        assertThat(get("/api/v1/me", null).headers().firstValue("Set-Cookie")).isEmpty();
    }

    @Test
    void aStoppedServerCannotBeStartedAgain() {
        start();
        server.stop();
        server.stop();

        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("build a new server");
    }

    @Test
    void aServerStoppedBeforeItStartedCannotBeStarted() {
        server = builder().build();

        server.stop();

        assertThatThrownBy(server::start).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void startOnAStartedServerDoesNothing() throws Exception {
        start();
        var port = server.port();

        server.start();

        assertThat(server.port()).isEqualTo(port);
        assertThat(get("/api/v1/me", admin()).statusCode()).isEqualTo(200);
    }

    @Test
    void aServerInTryWithResourcesIsStoppedAtTheEndOfTheBlock() {
        AdminHttpServer seen;
        try (var s = builder().build()) {
            s.start();
            seen = s;
        }

        assertThatThrownBy(seen::port).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(seen::start).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void signInsAndChangesAreLoggedWithTheirUserAndWithoutSecretsOrGuessedNames() throws Exception {
        var logs = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        logs.start();
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(Loggers.ADMIN_AUDIT);
        logger.addAppender(logs);
        try {
            start();
            assertThat(rawLogin("alice", "wrong-pass").statusCode()).isEqualTo(401);
            assertThat(rawLogin("mallory", "s3cret").statusCode()).isEqualTo(401);
            var admin = admin();
            var viewer = viewer();
            assertThat(mutate("POST", "/api/v1/tasks/1/retry", admin, "{\"revision\":\"7\"}")
                            .statusCode())
                    .isEqualTo(200);
            assertThat(mutate("POST", "/api/v1/schedules/nightly/pause", viewer, "{\"revision\":\"5\"}")
                            .statusCode())
                    .isEqualTo(403);
            assertThat(mutate("POST", "/api/v1/tasks/by-key/cancel?key=customer-7", admin, "{}")
                            .statusCode())
                    .isEqualTo(404);
            get("/api/v1/tasks/1", admin);

            var expected = List.of(
                    "Admin sign-in of alice (ADMIN) from 127.0.0.1",
                    "Admin sign-in of victor (VIEWER) from 127.0.0.1",
                    "Admin action POST /api/v1/tasks/1/retry by alice (ADMIN) answered 200",
                    "Admin action POST /api/v1/schedules/{}/pause by victor (VIEWER) answered 403",
                    "Admin action POST /api/v1/tasks/by-key/cancel by alice (ADMIN) answered 404");
            var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (!messages(logs).containsAll(expected) && System.nanoTime() < deadline) Thread.sleep(10);
            var lines = messages(logs);
            assertThat(lines).containsAll(expected);
            var refused = lines.stream()
                    .filter(l -> l.startsWith("Admin sign-in refused for the user with key "))
                    .toList();
            assertThat(refused).hasSize(2).allMatch(l -> l.endsWith(", from 127.0.0.1"));
            assertThat(refused.get(0))
                    .as("a known and an unknown name read alike")
                    .hasSameSizeAs(refused.get(1));
            assertThat(String.join("\n", lines))
                    .doesNotContain("mallory", "wrong-pass", "s3cret", "customer-7", "nightly", admin.csrf())
                    .doesNotContain(admin.cookie().substring(admin.cookie().indexOf('=') + 1));
            assertThat(lines).as("reads are not logged").noneMatch(l -> l.contains(" GET "));
        } finally {
            logger.detachAppender(logs);
        }
    }

    @Test
    void aWarnLevelOnTheServerClassDoesNotSilenceTheAuditLines() throws Exception {
        var logs = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        logs.start();
        var audit = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(Loggers.ADMIN_AUDIT);
        var serverClass = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(AdminHttpServer.class);
        var before = serverClass.getLevel();
        audit.addAppender(logs);
        serverClass.setLevel(ch.qos.logback.classic.Level.WARN);
        try {
            start();
            assertThat(rawLogin("alice", "s3cret").statusCode()).isEqualTo(200);

            assertThat(messages(logs)).contains("Admin sign-in of alice (ADMIN) from 127.0.0.1");
        } finally {
            serverClass.setLevel(before);
            audit.detachAppender(logs);
        }
    }

    private static List<String> messages(
            ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> logs) {
        return List.copyOf(logs.list).stream()
                .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
                .toList();
    }

    @Test
    void aBlockedSignInIsLoggedOncePerBlockWithItsAddressAndNeverWithTheName() throws Exception {
        var logs = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        logs.start();
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(Loggers.ADMIN_AUDIT);
        logger.addAppender(logs);
        try {
            start(builder().addTrustedProxy("127.0.0.1"));
            for (var i = 1; i <= 5; i++) {
                assertThat(loginFrom("203.0.113." + i, "alice", "bad")).isEqualTo(401);
                assertThat(loginFrom("198.51.100.9", "nobody" + i, "bad")).isEqualTo(401);
            }
            for (var i = 0; i < 3; i++) {
                assertThat(loginFrom("203.0.113.5" + i, "alice", "s3cret")).isEqualTo(429);
                assertThat(loginFrom("198.51.100.9", "victor", "view")).isEqualTo(429);
            }

            var lines = messages(logs);
            assertThat(lines)
                    .filteredOn(l -> l.equals("Admin sign-in refused, too many attempts from 198.51.100.9"))
                    .as("three attempts against the block of the address, one line")
                    .hasSize(1);
            assertThat(lines)
                    .filteredOn(l -> l.startsWith("Admin sign-in refused, too many attempts for the user with key "))
                    .singleElement()
                    .asString()
                    .endsWith(", from 203.0.113.50")
                    .doesNotContain("alice");
            assertThat(lines).noneMatch(l -> l.contains("nobody") || l.contains("alice") || l.contains("victor"));
        } finally {
            logger.detachAppender(logs);
        }
    }

    @Test
    void theRouteOfALoggedActionKeepsATaskIdAndHidesEveryOtherPathParameter() {
        assertThat(AdminHttpServer.auditedRoute("/api/v1/tasks/7/cancel")).isEqualTo("/api/v1/tasks/7/cancel");
        assertThat(AdminHttpServer.auditedRoute("/api/v1/tasks/7")).isEqualTo("/api/v1/tasks/7");
        assertThat(AdminHttpServer.auditedRoute("/api/v1/tasks/7x/cancel")).isEqualTo("/api/v1/tasks/{}/cancel");
        assertThat(AdminHttpServer.auditedRoute("/api/v1/tasks/by-key/cancel"))
                .isEqualTo("/api/v1/tasks/by-key/cancel");
        assertThat(AdminHttpServer.auditedRoute("/api/v1/queues/mail/pause")).isEqualTo("/api/v1/queues/{}/pause");
        assertThat(AdminHttpServer.auditedRoute("/api/v1/schedules/7/pause")).isEqualTo("/api/v1/schedules/{}/pause");
        assertThat(AdminHttpServer.auditedRoute("/api/v1/schedules/7")).isEqualTo("/api/v1/schedules/{}");
    }

    @Test
    void theLoggedRouteHidesPathParameters() {
        assertThat(AdminHttpServer.routeTemplate("/api/v1/tasks/by-key/cancel"))
                .isEqualTo("/api/v1/tasks/by-key/cancel");
        assertThat(AdminHttpServer.routeTemplate("/api/v1/tasks/by-key")).isEqualTo("/api/v1/tasks/by-key");
        assertThat(AdminHttpServer.routeTemplate("/api/v1/tasks/by-key/customer-7"))
                .isEqualTo("/api/v1/tasks/by-key/{}");
        assertThat(AdminHttpServer.routeTemplate("/api/v1/tasks/by-key/customer-7/cancel"))
                .isEqualTo("/api/v1/tasks/by-key/{}/cancel");
        assertThat(AdminHttpServer.routeTemplate("/api/v1/tasks/7/payload")).isEqualTo("/api/v1/tasks/{}/payload");
        assertThat(AdminHttpServer.routeTemplate("/api/v1/health")).isEqualTo("/api/v1/health");
        assertThat(AdminHttpServer.routeTemplate("/secret/file")).doesNotContain("secret");
    }

    @Test
    void concurrentLoginAttemptsCannotAllPassABlock() throws Exception {
        var throttle = new LoginThrottle(clock);
        var allowed = new java.util.concurrent.atomic.AtomicInteger();
        try (var pool = java.util.concurrent.Executors.newFixedThreadPool(16)) {
            var start = new java.util.concurrent.CountDownLatch(1);
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (var i = 0; i < 64; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    if (throttle.attempt("10.0.0.1") == 0) allowed.incrementAndGet();
                    return null;
                }));
            }
            start.countDown();
            for (var f : futures) f.get();
        }

        assertThat(allowed.get()).isEqualTo(5);
        assertThat(throttle.attempt("10.0.0.1")).isPositive();
    }

    @Test
    void aSuccessfulLoginTakesBackOnlyItsOwnAttempt() {
        var throttle = new LoginThrottle(clock);
        for (var i = 0; i < 3; i++) assertThat(throttle.attempt("a")).isZero();
        assertThat(throttle.attempt("a")).isZero();
        throttle.takeBack("a");
        assertThat(throttle.attempt("a")).as("fourth failure").isZero();
        assertThat(throttle.attempt("a")).as("fifth attempt, it sets the block").isZero();
        throttle.takeBack("a");
        assertThat(throttle.attempt("a"))
                .as("the fifth succeeded, so its block is lifted")
                .isZero();
        assertThat(throttle.attempt("a"))
                .as("but the failures before it still count")
                .isPositive();
    }

    @Test
    void atTheTrackingCapTheOldestOfTheKeysWithEquallyManyFailuresAreEvictedAndBlocksStay() {
        var throttle = new LoginThrottle(clock);
        for (var i = 0; i < 5; i++) throttle.attempt("blocked");
        throttle.attempt("old");
        clock.advance(Duration.ofMillis(1));
        for (var i = 1; i < 9_998; i++) throttle.attempt("addr-" + i);
        clock.advance(Duration.ofMillis(1));
        throttle.attempt("new");
        clock.advance(Duration.ofMillis(1));

        throttle.attempt("one-more");

        assertThat(throttle.attempt("blocked")).as("an active block stays").isPositive();
        for (var i = 0; i < 5; i++) {
            assertThat(throttle.attempt("old"))
                    .as("the oldest entry went, so its count restarted")
                    .isZero();
        }
        for (var i = 0; i < 4; i++) {
            assertThat(throttle.attempt("new")).as("a recent entry stays").isZero();
        }
        assertThat(throttle.attempt("new")).isPositive();
    }

    @Test
    void atTheTrackingCapAKeyOneFailureShortOfABlockOutlivesNewerKeysThatWereTriedOnce() {
        var throttle = new LoginThrottle(clock);
        for (var i = 0; i < 4; i++) throttle.attempt("guessed");
        clock.advance(Duration.ofMillis(1));
        for (var i = 0; i < 9_999; i++) throttle.attempt("made-up-" + i);
        clock.advance(Duration.ofMillis(1));

        throttle.attempt("one-more");

        assertThat(throttle.attempt("guessed")).as("the fifth failure").isZero();
        assertThat(throttle.attempt("guessed"))
                .as("its four failures were kept, so the fifth blocked it")
                .isEqualTo(10_000);
    }

    @Test
    void atTheTrackingCapAKeptKeyStaysWhereAnotherKeyWithTheSameFailuresIsForgotten() {
        var throttle = new LoginThrottle(clock, Set.of("real"), 100);
        for (var i = 0; i < 4; i++) {
            throttle.attempt("real");
            throttle.attempt("made-up");
        }
        clock.advance(Duration.ofMillis(1));
        for (var key = 0; key < 98; key++) {
            for (var i = 0; i < 4; i++) throttle.attempt("flood-" + key);
        }

        throttle.attempt("one-more");

        assertThat(throttle.attempt("real")).as("the fifth failure").isZero();
        assertThat(throttle.attempt("real")).as("its four failures were kept").isEqualTo(10_000);
        for (var i = 0; i < 5; i++) {
            assertThat(throttle.attempt("made-up"))
                    .as("the oldest key that is not kept went, so its count restarted")
                    .isZero();
        }
    }

    @Test
    void aThrottleFullOfBlockedKeysHoldsNoMoreAndLetsAnotherKeyThroughUncounted() {
        var throttle = new LoginThrottle(clock, Set.of("real"), 3);
        for (var key : List.of("a", "b", "c")) {
            for (var i = 0; i < 5; i++) throttle.attempt(key);
        }

        for (var i = 0; i < 12; i++) {
            assertThat(throttle.attempt("d"))
                    .as("not counted, so never blocked")
                    .isZero();
        }
        assertThat(throttle.size()).isEqualTo(3);
        assertThat(throttle.attempt("a"))
                .as("a block is not lifted to make room")
                .isPositive();

        for (var i = 0; i < 5; i++) {
            assertThat(throttle.attempt("real"))
                    .as("failure %d of a named key", i + 1)
                    .isZero();
        }
        assertThat(throttle.attempt("real"))
                .as("a named key is counted however full")
                .isEqualTo(10_000);
        assertThat(throttle.size()).isEqualTo(4);

        clock.advance(Duration.ofSeconds(11));
        for (var i = 0; i < 5; i++) throttle.attempt("d");
        assertThat(throttle.attempt("d"))
                .as("counted again once a block has ended")
                .isPositive();
        assertThat(throttle.size()).isLessThanOrEqualTo(4);
    }

    @Test
    void concurrentFirstAttemptsOfManyKeysCannotPassTheCapacityTogether() throws Exception {
        var throttle = new LoginThrottle(clock, Set.of(), 8);
        for (var key = 0; key < 8; key++) throttle.attempt("known-" + key);
        try (var pool = java.util.concurrent.Executors.newFixedThreadPool(16)) {
            var start = new java.util.concurrent.CountDownLatch(1);
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (var i = 0; i < 64; i++) {
                var key = "new-" + i;
                futures.add(pool.submit(() -> {
                    start.await();
                    throttle.attempt(key);
                    return null;
                }));
            }
            start.countDown();
            for (var f : futures) f.get();
        }

        assertThat(throttle.size()).isEqualTo(8);
    }

    @Test
    void floodingTheServerWithOtherNamesDoesNotTakeAwayTheFailuresOfAUserThatExists() throws Exception {
        start(builder().addTrustedProxy("127.0.0.1").loginThrottleCapacity(20));
        for (var i = 1; i <= 4; i++) {
            assertThat(loginFrom("203.0.113." + i, "alice", "bad")).isEqualTo(401);
        }
        clock.advance(Duration.ofMillis(1));
        for (var name = 0; name < 25; name++) {
            for (var i = 0; i < 4; i++) {
                assertThat(loginFrom("198.51." + name + "." + i, "made-up-" + name, "bad"))
                        .isEqualTo(401);
            }
        }

        assertThat(loginFrom("203.0.113.5", "alice", "bad"))
                .as("the fifth failure")
                .isEqualTo(401);
        assertThat(loginFrom("203.0.113.6", "alice", "s3cret"))
                .as("the four failures before the flood were kept")
                .isEqualTo(429);
    }

    @Test
    void theFirstRefusalOfABlockIsToldOnceAndAgainForTheNextBlock() {
        var throttle = new LoginThrottle(clock);
        assertThat(failFive(throttle, "key")).isEqualTo(10_000);

        assertThat(throttle.firstRefusal("key")).isTrue();
        assertThat(throttle.attempt("key")).isPositive();
        assertThat(throttle.firstRefusal("key")).isFalse();

        clock.advance(Duration.ofSeconds(10).plusMillis(1));
        assertThat(failFive(throttle, "key")).isEqualTo(20_000);
        assertThat(throttle.firstRefusal("key")).isTrue();
        assertThat(throttle.firstRefusal("key")).isFalse();
        assertThat(throttle.firstRefusal("never-seen")).isFalse();
    }

    private static long failFive(LoginThrottle throttle, String key) {
        for (var i = 0; i < 5; i++)
            assertThat(throttle.attempt(key)).as("attempt %d", i + 1).isZero();
        return throttle.attempt(key);
    }

    @Test
    void eachFurtherBlockOfAKeyLastsTwiceAsLongUpToFifteenMinutes() {
        var throttle = new LoginThrottle(clock);
        var expected = List.of(10L, 20L, 40L, 80L, 160L, 320L, 640L, 900L, 900L, 900L);
        for (var seconds : expected) {
            assertThat(failFive(throttle, "a")).isEqualTo(seconds * 1000);
            clock.advance(Duration.ofSeconds(seconds).plusMillis(1));
        }
    }

    @Test
    void aKeyStartsAgainAtTenSecondsAfterFifteenQuietMinutesAndNotBefore() {
        var throttle = new LoginThrottle(clock);
        for (var i = 0; i < 3; i++) {
            failFive(throttle, "a");
            clock.advance(Duration.ofSeconds(10L << i).plusMillis(1));
        }
        failFive(throttle, "a");
        clock.advance(Duration.ofSeconds(80).plus(Duration.ofMinutes(15)).minusMillis(1));
        assertThat(failFive(throttle, "a"))
                .as("a block of 160 s follows the one of 80 s")
                .isEqualTo(160_000);

        clock.advance(Duration.ofSeconds(160).plus(Duration.ofMinutes(15)).plusMillis(1));
        assertThat(failFive(throttle, "a")).isEqualTo(10_000);
    }

    @Test
    void attemptsAgainstABlockNeitherLengthenItNorMoveTheNextOne() {
        var throttle = new LoginThrottle(clock);
        assertThat(failFive(throttle, "a")).isEqualTo(10_000);
        for (var i = 0; i < 50; i++) {
            clock.advance(Duration.ofMillis(100));
            assertThat(throttle.attempt("a")).isEqualTo(10_000 - (i + 1) * 100L);
        }
        clock.advance(Duration.ofMillis(5_001));

        assertThat(failFive(throttle, "a")).isEqualTo(20_000);
    }

    @Test
    void aSuccessfulLoginNeitherKeepsNorDropsTheStepOfItsKey() {
        var throttle = new LoginThrottle(clock);
        failFive(throttle, "a");
        clock.advance(Duration.ofSeconds(11));
        assertThat(throttle.attempt("a")).isZero();
        throttle.takeBack("a");
        assertThat(failFive(throttle, "a")).as("the step is kept").isEqualTo(20_000);

        clock.advance(Duration.ofSeconds(21));
        clock.advance(Duration.ofMinutes(14));
        assertThat(throttle.attempt("a")).isZero();
        throttle.takeBack("a");
        clock.advance(Duration.ofMinutes(2));
        assertThat(failFive(throttle, "a"))
                .as("a success is no failed login, so it does not hold the step")
                .isEqualTo(10_000);
    }

    @Test
    void aSuccessThatSetTheFifthBlockLeavesTheStepWhereItWas() {
        var throttle = new LoginThrottle(clock);
        failFive(throttle, "a");
        clock.advance(Duration.ofSeconds(11));
        for (var i = 0; i < 4; i++) assertThat(throttle.attempt("a")).isZero();
        assertThat(throttle.attempt("a")).as("the fifth sets a block of 20 s").isZero();
        throttle.takeBack("a");

        assertThat(throttle.attempt("a")).as("the success lifted it").isZero();
        assertThat(throttle.attempt("a")).isEqualTo(20_000);
    }

    @Test
    void aLongBlockOfOneKeyDoesNotTouchAnotherKeyAndASuccessForAnotherKeyDoesNotShortenIt() {
        var throttle = new LoginThrottle(clock);
        for (var i = 0; i < 3; i++) {
            failFive(throttle, "alice");
            clock.advance(Duration.ofSeconds(10L << i).plusMillis(1));
        }
        assertThat(failFive(throttle, "alice")).isEqualTo(80_000);

        assertThat(throttle.attempt("victor")).isZero();
        throttle.takeBack("victor");
        assertThat(throttle.attempt("alice")).isEqualTo(80_000);
        throttle.takeBack("alice-never-seen");
        assertThat(throttle.attempt("alice")).isEqualTo(80_000);
    }

    @Test
    void floodingTheMapWithNewKeysNeitherLiftsABlockNorForgetsABlockedKeysStep() {
        var throttle = new LoginThrottle(clock);
        for (var i = 0; i < 2; i++) {
            failFive(throttle, "history");
            clock.advance(Duration.ofSeconds(10L << i).plusMillis(1));
        }
        assertThat(failFive(throttle, "blocked")).isEqualTo(10_000);
        clock.advance(Duration.ofSeconds(40).plusMillis(1));
        assertThat(failFive(throttle, "blocked")).isEqualTo(20_000);

        for (var i = 0; i < 40_000; i++) {
            throttle.attempt("flood-" + i);
            if (i % 1_000 == 0) clock.advance(Duration.ofMillis(1));
        }

        assertThat(throttle.attempt("blocked")).as("an active block stays").isPositive();
        for (var i = 0; i < 4; i++) assertThat(throttle.attempt("history")).isZero();
        assertThat(throttle.attempt("history"))
                .as("it still counts as blocked once already")
                .isZero();
        assertThat(throttle.attempt("history")).isEqualTo(40_000);
    }

    @Test
    void theRetryAfterOfAGrownBlockGrowsAndOnlyItsOwnUserWaits() throws Exception {
        start(builder().addTrustedProxy("127.0.0.1"));
        for (var round = 0; round < 2; round++) {
            for (var i = 1; i <= 5; i++) {
                assertThat(loginFrom("203.0.113." + (round * 10 + i), "alice", "bad"))
                        .isEqualTo(401);
            }
            var blocked = send(
                    "POST",
                    "/api/v1/session",
                    null,
                    Json.write(Map.of("user", "alice", "password", "s3cret")),
                    "X-Forwarded-For",
                    "203.0.113.99");
            assertThat(blocked.statusCode()).isEqualTo(429);
            assertThat(blocked.headers().firstValue("Retry-After")).hasValue(round == 0 ? "10" : "20");
            clock.advance(Duration.ofSeconds(11));
        }

        assertThat(loginFrom("203.0.113.99", "alice", "s3cret"))
                .as("20 s have not passed")
                .isEqualTo(429);
        assertThat(loginFrom("203.0.113.99", "victor", "view")).isEqualTo(200);
        clock.advance(Duration.ofSeconds(10));
        assertThat(loginFrom("203.0.113.99", "alice", "s3cret")).isEqualTo(200);
    }

    @Test
    void concurrentSessionCreationNeverExceedsTheCap() throws Exception {
        var sessions = new Sessions(clock, Duration.ofHours(1), 10);
        try (var pool = java.util.concurrent.Executors.newFixedThreadPool(16)) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (var i = 0; i < 400; i++) {
                futures.add(pool.submit(() -> sessions.create("u", Role.ADMIN)));
            }
            for (var f : futures) f.get();
        }

        assertThat(sessions.size()).isEqualTo(10);
    }

    @Test
    void aUserAtTheirSessionCapLosesTheirOwnOldestAndNoOneElses() {
        var sessions = new Sessions(clock, Duration.ofHours(1), 1000);
        var admin = sessions.create("admin", Role.ADMIN);
        clock.advance(Duration.ofSeconds(1));
        var firstViewer = sessions.create("viewer", Role.VIEWER);
        for (var i = 0; i < 2 * Sessions.PER_USER; i++) {
            clock.advance(Duration.ofSeconds(1));
            sessions.create("viewer", Role.VIEWER);
        }

        assertThat(sessions.size()).isEqualTo(1 + Sessions.PER_USER);
        assertThat(sessions.find(admin.token())).isNotNull();
        assertThat(sessions.find(firstViewer.token())).isNull();
    }

    @Test
    void aSignInDropsTheSessionsWhoseLifetimeHasPassedAlsoWhenTheyWereUsed() {
        var sessions = new Sessions(clock, Duration.ofHours(1), 1000);
        var used = sessions.create("a", Role.ADMIN);
        clock.advance(Duration.ofMinutes(59));
        assertThat(sessions.find(used.token())).isNotNull();
        clock.advance(Duration.ofMinutes(1));

        sessions.create("b", Role.ADMIN);

        assertThat(sessions.size()).isEqualTo(1);
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
                5,
                "send-mail",
                "default",
                null,
                null,
                null,
                Misfire.SKIP,
                Duration.ofHours(1),
                Overlap.SKIP,
                false,
                Instant.parse("2026-02-01T00:00:00Z"),
                3,
                Duration.ofMinutes(10));
    }
}
