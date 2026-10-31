package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * What a producer asks {@link TaskClient#enqueue} to store. Build one with {@link #of(TaskType, Object)} or
 * {@link #of(TaskType)} and the fluent setters; the request is immutable once built.
 *
 * @param <T> the payload type
 */
public final class TaskRequest<T> {

    public static final int MAX_UNIQUE_KEY_LENGTH = 256;
    public static final int MAX_ACCOUNT_LENGTH = 128;
    public static final int MAX_HEADERS_BYTES = 8 * 1024;
    public static final int MAX_PAYLOAD_BYTES = 256 * 1024;

    private final TaskType<T> type;
    private final T payload;
    private final String queue;
    private final String account;
    private final Instant at;
    private final Duration delay;
    private final String uniqueKey;
    private final Uniqueness uniqueness;
    private final Integer maxAttempts;
    private final Map<String, String> headers;
    private final Instant startBy;
    private final Duration maxRun;
    private final String retryPolicy;
    private final Long id;

    private TaskRequest(Builder<T> b) {
        this.type = b.type;
        this.payload = b.payload;
        this.queue = b.queue;
        this.account = b.account;
        this.at = b.at;
        this.delay = b.delay;
        this.uniqueKey = b.uniqueKey;
        this.uniqueness = b.uniqueness;
        this.maxAttempts = b.maxAttempts;
        this.headers = b.headers == null ? Map.of() : Map.copyOf(b.headers);
        this.startBy = b.startBy;
        this.maxRun = b.maxRun;
        this.retryPolicy = b.retryPolicy;
        this.id = b.id;
    }

    public static <T> Builder<T> of(TaskType<T> type, T payload) {
        if (type == null) throw new TaskValidationException("type is required");
        if (type.hasPayload() && payload == null) {
            throw new TaskValidationException("Task type " + type + " needs a payload");
        }
        return new Builder<>(type, payload);
    }

    public static Builder<Void> of(TaskType<Void> type) {
        if (type == null) throw new TaskValidationException("type is required");
        return new Builder<>(type, null);
    }

    public TaskType<T> type() {
        return type;
    }

    public T payload() {
        return payload;
    }

    /** @return the queue name, or null for the client's default queue */
    public String queue() {
        return queue;
    }

    public String account() {
        return account;
    }

    /** @return the absolute run time, or null */
    public Instant at() {
        return at;
    }

    /** @return the delay before the task may run, or null */
    public Duration delay() {
        return delay;
    }

    public String uniqueKey() {
        return uniqueKey;
    }

    public Uniqueness uniqueness() {
        return uniqueness;
    }

    /** @return the attempts allowed, or null for the client's default */
    public Integer maxAttempts() {
        return maxAttempts;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public Instant startBy() {
        return startBy;
    }

    public Duration maxRun() {
        return maxRun;
    }

    public String retryPolicy() {
        return retryPolicy;
    }

    /** @return the id the caller reserved for this task, or null when the client allocates one */
    public Long id() {
        return id;
    }

    public static final class Builder<T> {
        private final TaskType<T> type;
        private final T payload;
        private String queue;
        private String account;
        private Instant at;
        private Duration delay;
        private String uniqueKey;
        private Uniqueness uniqueness = Uniqueness.UNTIL_DONE;
        private Integer maxAttempts;
        private Map<String, String> headers;
        private Instant startBy;
        private Duration maxRun;
        private String retryPolicy;
        private Long id;

        private Builder(TaskType<T> type, T payload) {
            this.type = type;
            this.payload = payload;
        }

        public Builder<T> queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder<T> account(String account) {
            if (account != null && account.length() > MAX_ACCOUNT_LENGTH) {
                throw new TaskValidationException("account longer than " + MAX_ACCOUNT_LENGTH);
            }
            this.account = account;
            return this;
        }

        /** Runs the task at the given time instead of as soon as possible. */
        public Builder<T> at(Instant at) {
            this.at = at;
            this.delay = null;
            return this;
        }

        /** Runs the task after the given delay. */
        public Builder<T> delay(Duration delay) {
            if (delay != null && delay.isNegative()) throw new TaskValidationException("delay cannot be negative");
            this.delay = delay;
            this.at = null;
            return this;
        }

        /**
         * Makes the task unique: while the key is taken, another request with the same key is answered with
         * {@link EnqueueResult.Duplicate} instead of being stored.
         */
        public Builder<T> uniqueKey(String key) {
            if (key != null && (key.isBlank() || key.length() > MAX_UNIQUE_KEY_LENGTH)) {
                throw new TaskValidationException("uniqueKey must be 1 to " + MAX_UNIQUE_KEY_LENGTH + " characters");
            }
            this.uniqueKey = key;
            return this;
        }

        public Builder<T> uniqueKey(String key, Uniqueness uniqueness) {
            uniqueKey(key);
            if (uniqueness == null) throw new TaskValidationException("uniqueness is required");
            this.uniqueness = uniqueness;
            return this;
        }

        /** @param maxAttempts how often the task may be tried before it is dead, at least 1 */
        public Builder<T> maxAttempts(int maxAttempts) {
            if (maxAttempts < 1 || maxAttempts > Short.MAX_VALUE) {
                throw new TaskValidationException("maxAttempts must be between 1 and " + Short.MAX_VALUE);
            }
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder<T> header(String name, String value) {
            if (name == null || name.isBlank() || value == null) {
                throw new TaskValidationException("header name and value are required");
            }
            if (headers == null) headers = new LinkedHashMap<>();
            headers.put(name, value);
            return this;
        }

        public Builder<T> headers(Map<String, String> headers) {
            if (headers != null) headers.forEach(this::header);
            return this;
        }

        /** Reserved: stored with the task, acted on by a later version (expiry when not started by then). */
        public Builder<T> startBy(Instant startBy) {
            this.startBy = startBy;
            return this;
        }

        /** Reserved: stored with the task, acted on by a later version (fail an attempt that runs longer). */
        public Builder<T> maxRun(Duration maxRun) {
            if (maxRun != null && (maxRun.isNegative() || maxRun.isZero())) {
                throw new TaskValidationException("maxRun must be positive");
            }
            this.maxRun = maxRun;
            return this;
        }

        /** Reserved: stored with the task; this version applies the runner's retry policy to every task. */
        public Builder<T> retryPolicy(String name) {
            this.retryPolicy = name;
            return this;
        }

        /**
         * Uses an id obtained from {@link TaskClient#nextId()}. Enqueueing the same id twice stores the task once,
         * which makes a retry after a lost reply safe.
         */
        public Builder<T> id(long id) {
            if (id <= 0) throw new TaskValidationException("id must be positive");
            this.id = id;
            return this;
        }

        public TaskRequest<T> build() {
            return new TaskRequest<>(this);
        }
    }
}
