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

    /**
     * The largest {@code maxAttempts} of a task or schedule, the way to say "retry until it succeeds". With the
     * default retry policy (1 s doubling to a one-hour cap) it amounts to about 3.7 years of hourly retries.
     */
    public static final int MAX_ATTEMPTS = Short.MAX_VALUE;

    /** The longest {@code maxRunTime} of a task or schedule, about 24.8 days: stores keep it as an int of milliseconds. */
    public static final Duration MAX_RUN_TIME = Duration.ofMillis(Integer.MAX_VALUE);

    public static final int MAX_UNIQUE_KEY_LENGTH = 256;
    public static final int MAX_ACCOUNT_LENGTH = 128;
    public static final int MAX_HEADERS_BYTES = 8 * 1024;
    public static final int MAX_PAYLOAD_BYTES = 256 * 1024;

    /**
     * How far from now, either way, a time handed to a store may lie, and the longest duration a store adds to
     * or takes from now: 36,525 days (100 years), so that every store can keep it.
     */
    public static final Duration MAX_TIME_FROM_NOW = Duration.ofDays(36_525);

    /** The rule for a maximum run time of a task or schedule; null means none. */
    public static void requireMaxRunTime(Duration maxRunTime) {
        if (maxRunTime != null
                && (maxRunTime.isNegative() || maxRunTime.isZero() || maxRunTime.compareTo(MAX_RUN_TIME) > 0)) {
            throw new TaskValidationException("maxRunTime must be positive and at most " + MAX_RUN_TIME);
        }
    }

    /** The rule for a unique key, for enqueueing and for looking one up. */
    public static void requireUniqueKey(String key) {
        if (key.isBlank() || key.length() > MAX_UNIQUE_KEY_LENGTH) {
            throw new TaskValidationException("uniqueKey must be 1 to " + MAX_UNIQUE_KEY_LENGTH + " characters");
        }
        StoredText.require("uniqueKey", key);
    }

    /** The rule for an account, for a task and for a schedule. */
    public static void requireAccount(String account) {
        if (account != null && account.length() > MAX_ACCOUNT_LENGTH) {
            throw new TaskValidationException("account longer than " + MAX_ACCOUNT_LENGTH);
        }
        StoredText.require("account", account);
    }

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
    private final Duration maxRunTime;
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
        this.maxRunTime = b.maxRunTime;
        this.retryPolicy = b.retryPolicy;
        this.id = b.id;
    }

    private TaskRequest(TaskRequest<T> source, long id) {
        this.type = source.type;
        this.payload = source.payload;
        this.queue = source.queue;
        this.account = source.account;
        this.at = source.at;
        this.delay = source.delay;
        this.uniqueKey = source.uniqueKey;
        this.uniqueness = source.uniqueness;
        this.maxAttempts = source.maxAttempts;
        this.headers = source.headers;
        this.startBy = source.startBy;
        this.maxRunTime = source.maxRunTime;
        this.retryPolicy = source.retryPolicy;
        this.id = positiveId(id);
    }

    private static long positiveId(long id) {
        if (id <= 0) throw new TaskValidationException("id must be positive");
        return id;
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

    public Duration maxRunTime() {
        return maxRunTime;
    }

    public String retryPolicy() {
        return retryPolicy;
    }

    /** @return the id the caller reserved for this task, or null when the client allocates one */
    public Long id() {
        return id;
    }

    /**
     * Copies this request with an allocated id; all other fields, including the payload, are preserved.
     *
     * @throws TaskValidationException when the id is not positive
     */
    public TaskRequest<T> withId(long id) {
        return new TaskRequest<>(this, id);
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
        private Duration maxRunTime;
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
            requireAccount(account);
            this.account = account;
            return this;
        }

        /**
         * Runs the task at the given time instead of as soon as possible; within {@link #MAX_TIME_FROM_NOW} of
         * now when the task is enqueued.
         */
        public Builder<T> at(Instant at) {
            this.at = at;
            this.delay = null;
            return this;
        }

        /** Runs the task after the given delay, at most {@link #MAX_TIME_FROM_NOW}. */
        public Builder<T> delay(Duration delay) {
            if (delay != null && delay.isNegative()) throw new TaskValidationException("delay cannot be negative");
            StoredTime.require("delay", delay);
            this.delay = delay;
            this.at = null;
            return this;
        }

        /**
         * Makes the task record unique: while the key is taken, another request with the same key is answered with
         * {@link EnqueueResult.Duplicate} instead of being stored. The key is taken until the task is done, also
         * when an earlier call on this builder asked for a keep window. See {@link Uniqueness} for lease-loss
         * limits.
         */
        public Builder<T> uniqueKey(String key) {
            if (key != null) requireUniqueKey(key);
            this.uniqueKey = key;
            this.uniqueness = Uniqueness.UNTIL_DONE;
            return this;
        }

        public Builder<T> uniqueKey(String key, Uniqueness uniqueness) {
            uniqueKey(key);
            if (uniqueness == null) throw new TaskValidationException("uniqueness is required");
            this.uniqueness = uniqueness;
            return this;
        }

        /** @param maxAttempts how often the task may be tried before it is dead, 1 to {@link #MAX_ATTEMPTS} */
        public Builder<T> maxAttempts(int maxAttempts) {
            if (maxAttempts < 1 || maxAttempts > MAX_ATTEMPTS) {
                throw new TaskValidationException("maxAttempts must be between 1 and " + MAX_ATTEMPTS);
            }
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder<T> header(String name, String value) {
            if (name == null || name.isBlank() || value == null) {
                throw new TaskValidationException("header name and value are required");
            }
            StoredText.require("header name", name);
            StoredText.require("header value", value);
            if (headers == null) headers = new LinkedHashMap<>();
            headers.put(name, value);
            return this;
        }

        public Builder<T> headers(Map<String, String> headers) {
            if (headers != null) headers.forEach(this::header);
            return this;
        }

        /**
         * The task ends EXPIRED without running when it has not been claimed by this time; within
         * {@link #MAX_TIME_FROM_NOW} of now when the task is enqueued.
         */
        public Builder<T> startBy(Instant startBy) {
            this.startBy = startBy;
            return this;
        }

        /** An attempt running longer than this is cancelled; the task ends CANCELLED. */
        public Builder<T> maxRunTime(Duration maxRunTime) {
            requireMaxRunTime(maxRunTime);
            this.maxRunTime = maxRunTime;
            return this;
        }

        /** Name of a retry policy registered with {@code BackgroundRunner.Builder#retryPolicy(String, RetryPolicy)}; an unknown name falls back to the default policy. */
        public Builder<T> retryPolicy(String name) {
            StoredText.require("retryPolicy", name);
            this.retryPolicy = name;
            return this;
        }

        /**
         * Uses an id allocated by a client of this namespace: {@link TaskClient#nextId()}, or an id an
         * {@link today.bonfire.oss.bth4j.exceptions.EnqueueFailedException} carries outside its
         * {@code notStored()} positions. Enqueueing the same request,
         * encoded identically, with the same id twice stores the task once, which makes a retry after a lost
         * reply safe. An id that was never allocated is rejected with a {@link TaskValidationException} when
         * the request is enqueued. An id that already belongs to a task is accepted again only when the stored
         * task has the same type, queue, payload (compared as stored text, when the task still has it) and
         * unique key (when the stored task still holds one); anything else throws a
         * {@link today.bonfire.oss.bth4j.exceptions.TaskIdReusedException}. Headers, account, attempts, retry
         * policy, start-by, max run, delay, {@code at(...)} time, {@code keepFor} duration and payload version
         * are not compared. Never invent an id.
         */
        public Builder<T> id(long id) {
            this.id = positiveId(id);
            return this;
        }

        public TaskRequest<T> build() {
            return new TaskRequest<>(this);
        }
    }
}
