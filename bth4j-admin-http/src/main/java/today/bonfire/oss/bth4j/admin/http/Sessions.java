package today.bonfire.oss.bth4j.admin.http;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory sessions with a sliding expiry and a hard cap that evicts the oldest session. */
final class Sessions {

    static final class Session {
        private final String token;
        private final String csrf;
        private final String user;
        private final Role role;
        private final long created;
        private volatile long lastSeen;

        private Session(String token, String csrf, String user, Role role, long now) {
            this.token = token;
            this.csrf = csrf;
            this.user = user;
            this.role = role;
            this.created = now;
            this.lastSeen = now;
        }

        String token() {
            return token;
        }

        String csrf() {
            return csrf;
        }

        String user() {
            return user;
        }

        Role role() {
            return role;
        }
    }

    private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();

    private final SecureRandom random = new SecureRandom();
    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
    private final Clock clock;
    private final long ttlMillis;
    private final int max;

    Sessions(Clock clock, Duration ttl, int max) {
        this.clock = clock;
        this.ttlMillis = ttl.toMillis();
        this.max = max;
    }

    Session create(String user, Role role) {
        var now = clock.millis();
        if (sessions.size() >= max) {
            evict(now);
        }
        var session = new Session(randomToken(), randomToken(), user, role, now);
        sessions.put(session.token, session);
        return session;
    }

    /** @return the live session for the token, whose expiry is pushed out, or null */
    Session find(String token) {
        if (token == null) {
            return null;
        }
        var session = sessions.get(token);
        if (session == null) {
            return null;
        }
        var now = clock.millis();
        if (now - session.lastSeen >= ttlMillis) {
            sessions.remove(token, session);
            return null;
        }
        session.lastSeen = now;
        return session;
    }

    void remove(String token) {
        sessions.remove(token);
    }

    int size() {
        return sessions.size();
    }

    private void evict(long now) {
        sessions.values().removeIf(s -> now - s.lastSeen >= ttlMillis);
        while (sessions.size() >= max) {
            sessions.values().stream()
                    .min(Comparator.comparingLong((Session s) -> s.created))
                    .ifPresent(s -> sessions.remove(s.token, s));
        }
    }

    private String randomToken() {
        var bytes = new byte[32];
        random.nextBytes(bytes);
        return B64.encodeToString(bytes);
    }
}
