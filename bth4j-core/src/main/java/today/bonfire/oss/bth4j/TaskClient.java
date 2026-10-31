package today.bonfire.oss.bth4j;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.common.Cron;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.EnqueueFailedException;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.service.Codecs;
import today.bonfire.oss.bth4j.service.DefaultAdminOps;
import today.bonfire.oss.bth4j.service.IdAllocator;
import today.bonfire.oss.bth4j.service.Names;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;

/**
 * The producer side: enqueues tasks, manages recurring definitions and reads tasks. Thread-safe; one instance
 * per process and store is enough. A {@link BackgroundRunner} exposes its own through
 * {@link BackgroundRunner#client()}; a process that only produces builds one with {@link #builder()}.
 */
public final class TaskClient {
    private static final Logger log = LoggerFactory.getLogger(TaskClient.class);
    private static final int ENQUEUE_CHUNK = 500;

    private final FullStore store;
    private final Names names;
    private final IdAllocator ids;
    private final Codecs codecs;
    private final QueuesHolder queues;
    private final Clock clock;
    private final int defaultMaxAttempts;
    private final AdminOps admin;

    TaskClient(Builder b) {
        this.store = b.store;
        this.names = b.names != null ? b.names : new Names(b.store);
        this.ids = new IdAllocator(b.store, b.idBlockSize);
        this.codecs = new Codecs(b.jsonMapper);
        this.queues = b.queues;
        this.clock = b.clock;
        this.defaultMaxAttempts = b.defaultMaxAttempts;
        this.admin = new DefaultAdminOps(store, names, queues, clock, null, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Reserves an id for a task to be enqueued with {@link TaskRequest.Builder#id(long)}. Knowing the id
     * before the write lets a caller retry an enqueue whose reply was lost.
     */
    public long nextId() {
        return ids.nextId();
    }

    /**
     * Stores the task.
     *
     * @throws TaskValidationException when the request is invalid or exceeds a size limit
     * @throws EnqueueFailedException  when the store could not be reached; it carries the id to retry with
     */
    public EnqueueResult enqueue(TaskRequest<?> request) {
        var record = toRecord(request);
        try {
            return result(record.id(), store.enqueue(record));
        } catch (TaskStoreException e) {
            throw new EnqueueFailedException(List.of(record.id()), 0, e);
        }
    }

    /**
     * Stores several tasks in chunks. Every id is reserved before the first write; when a chunk fails the
     * exception carries the id of every request, so the caller can retry the same list with
     * {@link TaskRequest.Builder#id(long)} set from {@link EnqueueFailedException#ids()}: tasks already
     * stored answer accepted again instead of being stored twice.
     *
     * @return one result per request, in order
     */
    public List<EnqueueResult> enqueueAll(List<? extends TaskRequest<?>> requests) {
        var records = new ArrayList<TaskRecord>(requests.size());
        var ids = new ArrayList<Long>(requests.size());
        for (var r : requests) {
            var record = toRecord(r);
            records.add(record);
            ids.add(record.id());
        }
        var results = new ArrayList<EnqueueResult>(records.size());
        for (int from = 0; from < records.size(); from += ENQUEUE_CHUNK) {
            var chunk = records.subList(from, Math.min(records.size(), from + ENQUEUE_CHUNK));
            List<EnqueueOutcome> outcomes;
            try {
                outcomes = store.enqueueAll(chunk);
            } catch (TaskStoreException e) {
                throw new EnqueueFailedException(ids, results.size(), e);
            }
            for (int i = 0; i < chunk.size(); i++) {
                results.add(result(chunk.get(i).id(), outcomes.get(i)));
            }
        }
        return results;
    }

    /**
     * Validates and encodes a request into the record the store would receive, with its id allocated, for
     * callers that write the record themselves, for example inside their own database transaction with
     * {@code PostgresTaskStore.enqueue(Connection, TaskRecord)}.
     */
    public TaskRecord prepare(TaskRequest<?> request) {
        return toRecord(request);
    }

    /** Operator view and operations over this store; runner health and issues are empty here. */
    public AdminOps admin() {
        return admin;
    }

    private static EnqueueResult result(long id, EnqueueOutcome outcome) {
        return switch (outcome) {
            case EnqueueOutcome.Stored s -> new EnqueueResult.Accepted(id);
            case EnqueueOutcome.Exists e -> new EnqueueResult.Accepted(id);
            case EnqueueOutcome.DuplicateKey d -> new EnqueueResult.Duplicate(d.existingId());
        };
    }

    <T> TaskRecord toRecord(TaskRequest<T> request) {
        var type = request.type();
        var now = clock.instant();
        Instant due = request.at();
        if (due == null && request.delay() != null) due = now.plus(request.delay());
        if (request.startBy() != null && due != null && request.startBy().isBefore(due)) {
            throw new TaskValidationException("startBy is before the run time");
        }
        var b = TaskRecord.builder()
                .id(request.id() != null ? request.id() : ids.nextId())
                .queueId(names.queueId(queues.getValidQueue(request.queue())))
                .eventId(names.eventId(type.name()))
                .payloadVersion(type.payloadVersion())
                .state(due != null && due.isAfter(now) ? TaskState.SCHEDULED : TaskState.QUEUED)
                .maxAttempts(request.maxAttempts() != null ? request.maxAttempts() : defaultMaxAttempts)
                .created(now)
                .due(due)
                .data(codecs.encode(type, request.payload()))
                .account(request.account())
                .headers(validHeaders(request.headers()))
                .uniqueKey(request.uniqueKey())
                .retryPolicy(request.retryPolicy())
                .startBy(request.startBy())
                .maxRun(request.maxRun());
        if (request.uniqueKey() != null && request.uniqueness() instanceof Uniqueness.KeepFor k) {
            b.keyKeep(k.duration());
        }
        return b.build();
    }

    private static Map<String, String> validHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) return null;
        long bytes = 0;
        for (var e : headers.entrySet()) {
            bytes += e.getKey().getBytes(StandardCharsets.UTF_8).length
                    + e.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        if (bytes > TaskRequest.MAX_HEADERS_BYTES) {
            throw new TaskValidationException("headers exceed " + TaskRequest.MAX_HEADERS_BYTES + " bytes");
        }
        return headers;
    }

    /**
     * Creates or replaces a recurring definition. Re-registering with the same cron expression keeps the next
     * fire time; a changed expression resets it. A fixed-delay definition first fires one delay from now.
     *
     * @return the next fire time
     */
    public <T> Instant putSchedule(ScheduleDefinition<T> definition) {
        var now = clock.instant();
        Instant next;
        if (definition.isFixedDelay()) {
            next = now.plus(definition.fixedDelay());
        } else {
            Cron.validate(definition.cron());
            var zone = definition.zone();
            next = Cron.next(definition.cron(), now, zone)
                    .orElseThrow(
                            () -> new TaskValidationException("Cron expression never fires: " + definition.cron()));
        }
        var type = definition.type();
        var misfire = switch (definition.misfire()) {
            case Misfire.Skip s -> RecurringDefinition.MISFIRE_SKIP;
            case Misfire.FireOnceNow f -> RecurringDefinition.MISFIRE_FIRE_ONCE_NOW;
            case Misfire.CatchUp c -> RecurringDefinition.MISFIRE_CATCH_UP;
        };
        var stored = new RecurringDefinition(
                definition.id(),
                names.eventId(type.name()),
                type.payloadVersion(),
                names.queueId(queues.getValidQueue(definition.queue())),
                definition.account(),
                definition.cron(),
                definition.zone() == null ? null : definition.zone().getId(),
                misfire,
                definition.misfire() instanceof Misfire.CatchUp c ? c.max() : 0,
                definition.fixedDelay() == null ? null : definition.fixedDelay().toMillis(),
                definition.overlap() == Overlap.SKIP,
                codecs.encode(type, definition.payload()),
                definition.maxAttempts() != null ? definition.maxAttempts() : defaultMaxAttempts,
                false,
                0);
        store.putRecurring(stored, next);
        log.info(
                "Registered schedule {} ({}) next at {}",
                definition.id(),
                definition.isFixedDelay() ? "every " + definition.fixedDelay() : definition.cron(),
                next);
        return next;
    }

    /** @return true when a definition with that id existed */
    public boolean deleteSchedule(String id) {
        return store.deleteRecurring(id);
    }

    /** @return every definition's id with its next fire time */
    public Map<String, Instant> schedules() {
        return store.recurringSchedules();
    }

    /** @return the task with that id, in whatever state it is */
    public Optional<TaskView> task(long id) {
        return store.getTask(id).map(names::view);
    }

    /** @return counts per state for every queue that has or had tasks, by queue name */
    public Map<String, StateCounts> counts() {
        var result = new java.util.LinkedHashMap<String, StateCounts>();
        store.counts().forEach((id, c) -> result.put(names.queueName(id), c));
        return result;
    }

    /**
     * Lists tasks in one state, oldest state change first.
     *
     * @param queue   a queue name, or null for every queue
     * @param afterId for paging: only tasks after this one in the listing; 0 starts at the beginning
     */
    public List<TaskView> tasks(TaskState state, String queue, long afterId, int limit) {
        var queueId = queue == null ? null : Integer.valueOf(names.queueId(queues.getValidQueue(queue)));
        var result = new ArrayList<TaskView>();
        for (var r : store.tasks(state, queueId, afterId, limit)) result.add(names.view(r));
        return result;
    }

    public FullStore store() {
        return store;
    }

    public Names names() {
        return names;
    }

    public Codecs codecs() {
        return codecs;
    }

    public IdAllocator ids() {
        return ids;
    }

    public QueuesHolder queues() {
        return queues;
    }

    public Clock clock() {
        return clock;
    }

    public int defaultMaxAttempts() {
        return defaultMaxAttempts;
    }

    public static final class Builder {
        static final List<String> DEFAULT_QUEUES = List.of("Q:DEF", "Q:LOW", "Q:MED", "Q:HIGH");

        private FullStore store;
        private Names names;
        private JsonMapper jsonMapper;
        private Clock clock = Clock.systemUTC();
        private int idBlockSize = 64;
        private int defaultMaxAttempts = 10;
        private QueuesHolder queues = new QueuesHolder(new LinkedHashSet<>(DEFAULT_QUEUES), DEFAULT_QUEUES, "Q:DEF");

        private Builder() {}

        /** The store, for example a {@code RedisTaskStore} or {@code PostgresTaskStore}. Required. */
        public Builder store(FullStore store) {
            this.store = store;
            return this;
        }

        /** Mapper for JSON payloads and step results. Needed unless every task type has its own codec. */
        public Builder jsonMapper(JsonMapper jsonMapper) {
            this.jsonMapper = jsonMapper;
            return this;
        }

        public Builder clock(Clock clock) {
            if (clock == null) throw new TaskConfigurationError("clock cannot be null");
            this.clock = clock;
            return this;
        }

        /** How many ids are reserved per store call; 1 means one call per enqueue. Default 64. */
        public Builder idBlockSize(int size) {
            if (size < 1 || size > 100_000) throw new TaskConfigurationError("idBlockSize must be 1 to 100000");
            this.idBlockSize = size;
            return this;
        }

        /** Attempts for requests that do not set their own. Default 10. */
        public Builder defaultMaxAttempts(int attempts) {
            if (attempts < 1 || attempts > Short.MAX_VALUE) {
                throw new TaskConfigurationError("defaultMaxAttempts must be 1 to " + Short.MAX_VALUE);
            }
            this.defaultMaxAttempts = attempts;
            return this;
        }

        /**
         * The queues tasks may be enqueued to and the one used when a request names none. Default
         * {@code Q:DEF, Q:LOW, Q:MED, Q:HIGH} with {@code Q:DEF} as default.
         */
        public Builder queues(List<String> availableQueues, String defaultQueue) {
            if (availableQueues == null || availableQueues.isEmpty()) {
                throw new TaskConfigurationError("availableQueues cannot be empty");
            }
            for (var q : availableQueues) {
                if (StringUtils.isBlank(q)) throw new TaskConfigurationError("queue names cannot be blank");
            }
            Set<String> available = new LinkedHashSet<>(availableQueues);
            var def = StringUtils.isBlank(defaultQueue) ? availableQueues.getFirst() : defaultQueue;
            if (!available.contains(def)) throw new TaskConfigurationError("default queue is not an available queue");
            this.queues = new QueuesHolder(available, List.copyOf(available), def);
            return this;
        }

        Builder queues(QueuesHolder queues) {
            this.queues = queues;
            return this;
        }

        Builder names(Names names) {
            this.names = names;
            return this;
        }

        /** Builds the client and initializes the store schema. */
        public TaskClient build() {
            if (store == null) throw new TaskConfigurationError("store is required");
            store.init();
            return new TaskClient(this);
        }
    }
}
