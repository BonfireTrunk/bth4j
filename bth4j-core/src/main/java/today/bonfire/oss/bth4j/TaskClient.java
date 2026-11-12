package today.bonfire.oss.bth4j;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.common.Cron;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.exceptions.EnqueueFailedException;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskIdReusedException;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.PutRecurringOutcome;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * The producer side: enqueues tasks, manages recurring definitions and reads tasks. Thread-safe; one instance
 * per process and store is enough. A {@link BackgroundRunner} exposes its own through
 * {@link BackgroundRunner#client()}; a process that only produces builds one with {@link #builder()}.
 */
public final class TaskClient {
    private static final Logger log = LoggerFactory.getLogger(Loggers.CLIENT);
    private static final int ENQUEUE_CHUNK = 500;
    private static final int MAX_ID_TRIES = 3;
    private static final String ID_TAKEN = "generated task ids were already in use after " + MAX_ID_TRIES + " tries";

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
     * Stores the task. A task without a caller-set id gets a fresh id; should the store already hold a task under
     * it (another client's reserved id was set by hand), a new id is reserved and the task stored again, up to
     * three tries in all. An existing caller id with matching content answers {@code Accepted} without
     * starting another run, even when DEAD; use {@link AdminOps#retry(long, int)} to run a dead task again.
     *
     * @throws TaskValidationException when the request is invalid or exceeds a size limit, or sets an id that
     *                                 was never allocated
     * @throws TaskDataException       when the payload cannot be encoded
     * @throws TaskConfigurationException when the task type has no codec and this client has no JSON mapper
     * @throws TaskIdReusedException   when the request sets an id that belongs to a task with other content
     * @throws UnreadableTaskException when the stored task of the request's id cannot be read; nothing was written,
     *                                 and a retry fails the same way until that task is repaired or deleted
     * @throws EnqueueFailedException  when the store could not be reached or all three generated ids were taken;
     *                                 with an id the task may already be stored (retry with that id); with no
     *                                 ids nothing was written by this call (resend the same request)
     */
    public EnqueueResult enqueue(TaskRequest<?> request) {
        List<Long> known = request.id() == null ? List.of() : List.of(request.id());
        try {
            if (request.id() != null) {
                requireAllocated(request.id());
                var record = toRecord(request, request.id());
                return result(record, store.enqueue(record), false);
            }
            for (int tries = 1; tries <= MAX_ID_TRIES; tries++) {
                var record = toRecord(request, ids.nextId());
                known = List.of(record.id());
                var outcome = store.enqueue(record);
                if (!(outcome instanceof EnqueueOutcome.Exists)) return result(record, outcome, false);
                known = List.of();
                ids.abandonBlock();
                log.warn("Generated task id {} is already in use, reserving another", record.id());
            }
        } catch (TaskStoreException e) {
            throw new EnqueueFailedException(known, List.of(), e);
        }
        throw new EnqueueFailedException(known, List.of(), new TaskStoreException(ID_TAKEN));
    }

    /**
     * Stores several tasks in chunks. Initial ids are allocated before the first write; occupied generated ids
     * are replaced during writing. Every caller id is checked against its stored task before the first write.
     * Caller ids are validated against the high-water mark before fresh ids are reserved. A generated id
     * found taken abandons the current id block and is replaced from a fresh block, at most three writes
     * per request. On failure, {@link EnqueueFailedException#settled()} holds the results of the completed
     * chunks and {@link EnqueueFailedException#retryRequests(List)} prepares the other requests for retry: it
     * preserves ids of writes whose outcome is uncertain and caller-set ids, but leaves
     * generated requests known not to have been written without an id. These include collisions and
     * requests in unattempted chunks. No replacement ids are reserved after the third collision.
     * Retrying is idempotent while the original tasks are retained and the requests encode identically;
     * a task that ran and was removed can be stored again. Caller-id reuse still requires caller correction.
     *
     * @return one result per request, in order
     * @throws TaskValidationException when a request sets an id that was never allocated, or two requests set
     *                                 the same id, or a caller id overlaps this batch's fresh id range
     * @throws TaskIdReusedException   when a request sets an id that belongs to a task with other content;
     *                                 checked before the first write, so nothing is stored
     * @throws UnreadableTaskException when the stored task of a caller id cannot be read; checked before the
     *                                 first write, so nothing is stored
     * @throws EnqueueFailedException when the store fails before the first write, or anything fails from the
     *                                 first write on: the store, generated ids that cannot be replaced, caller-id
     *                                 reuse discovered after preflight, a stored task that cannot be read;
     *                                 carries the ids as described above and what failed as its cause
     */
    public List<EnqueueResult> enqueueAll(List<? extends TaskRequest<?>> requests) {
        var notStored = new HashSet<Integer>();
        for (int i = 0; i < requests.size(); i++) {
            if (requests.get(i).id() == null) notStored.add(i);
        }
        var callerIds = new HashSet<Long>();
        for (var r : requests) {
            if (r.id() != null && !callerIds.add(r.id())) {
                throw new TaskValidationException("id " + r.id() + " is set on more than one request");
            }
        }
        long nextId = 0;
        int freshCount = requests.size() - callerIds.size();
        try {
            callerIds.stream().mapToLong(Long::longValue).max().ifPresent(this::requireAllocated);
            if (freshCount > 0) nextId = this.ids.nextIds(freshCount);
        } catch (TaskStoreException e) {
            throw new EnqueueFailedException(List.of(), List.of(), notStored, e);
        }
        for (long id : callerIds) {
            if (id >= nextId && id < nextId + freshCount) {
                throw new TaskValidationException("id " + id + " overlaps the batch's freshly allocated ids");
            }
        }
        var ids = new ArrayList<Long>(requests.size());
        for (var r : requests) ids.add(r.id() != null ? r.id() : nextId++);
        var records = new ArrayList<TaskRecord>(requests.size());
        var verified = new HashSet<Long>();
        try {
            for (int i = 0; i < requests.size(); i++) records.add(toRecord(requests.get(i), ids.get(i)));
            for (int i = 0; i < requests.size(); i++) {
                if (requests.get(i).id() != null && requireSameContent(records.get(i))) {
                    verified.add(ids.get(i));
                }
            }
        } catch (TaskStoreException e) {
            throw new EnqueueFailedException(ids, List.of(), notStored, e);
        }
        var results = new ArrayList<EnqueueResult>(records.size());
        for (int from = 0; from < records.size(); from += ENQUEUE_CHUNK) {
            var to = Math.min(records.size(), from + ENQUEUE_CHUNK);
            try {
                results.addAll(writeChunk(requests, records, ids, callerIds, verified, notStored, from, to));
            } catch (RuntimeException e) {
                throw new EnqueueFailedException(ids, results, notStored, e);
            }
        }
        return results;
    }

    private List<EnqueueResult> writeChunk(
            List<? extends TaskRequest<?>> requests,
            List<TaskRecord> records,
            List<Long> ids,
            Set<Long> callerIds,
            Set<Long> verified,
            Set<Integer> notStored,
            int from,
            int to) {
        var out = new EnqueueResult[to - from];
        List<Integer> todo = new ArrayList<>();
        for (int i = 0; i < out.length; i++) todo.add(i);
        for (int tries = 1; ; tries++) {
            var sent = new ArrayList<TaskRecord>(todo.size());
            for (int i : todo) sent.add(records.get(from + i));
            for (int i : todo) notStored.remove(from + i);
            var outcomes = store.enqueueAll(sent);
            var taken = new ArrayList<Integer>();
            for (int k = 0; k < todo.size(); k++) {
                int i = todo.get(k);
                if (outcomes.get(k) instanceof EnqueueOutcome.Exists
                        && requests.get(from + i).id() == null) {
                    taken.add(i);
                    notStored.add(from + i);
                }
            }
            if (!taken.isEmpty()) this.ids.abandonBlock();
            for (int k = 0; k < todo.size(); k++) {
                int i = todo.get(k);
                if (!notStored.contains(from + i)) {
                    var record = sent.get(k);
                    out[i] = result(record, outcomes.get(k), verified.contains(record.id()));
                }
            }
            if (taken.isEmpty()) return Arrays.asList(out);
            if (tries == MAX_ID_TRIES) throw new TaskStoreException(ID_TAKEN);
            long first;
            try {
                first = this.ids.nextIds(taken.size());
            } catch (TaskStoreException e) {
                throw new TaskStoreException("could not replace occupied generated task ids", e);
            }
            for (long id : callerIds) {
                if (id >= first && id < first + taken.size()) {
                    throw new TaskStoreException("replacement task ids overlap a caller id");
                }
            }
            for (int k = 0; k < taken.size(); k++) {
                int at = from + taken.get(k);
                records.set(at, records.get(at).toBuilder().id(first + k).build());
                ids.set(at, first + k);
            }
            todo = taken;
        }
    }

    /**
     * Validates and encodes a request into the record the store would receive, with its id allocated, for
     * callers that write the record themselves, for example inside their own database transaction with
     * {@code PostgresTaskStore.enqueue(Connection, TaskRecord)}. This path skips the caller-id allocation
     * and stored-content checks performed by {@link #enqueue} and {@link #enqueueAll}; callers handle the
     * store's raw {@link EnqueueOutcome}, including {@link EnqueueOutcome.Exists}.
     */
    public TaskRecord prepare(TaskRequest<?> request) {
        return toRecord(request);
    }

    /** Operator view and operations over this store; runner health and issues are empty here. */
    public AdminOps admin() {
        return admin;
    }

    private void requireAllocated(long id) {
        if (id >= ids.knownEnd() && id >= store.idHighWaterMark()) {
            throw new TaskValidationException("id " + id + " was never allocated; take ids from nextId()");
        }
    }

    private EnqueueResult result(TaskRecord sent, EnqueueOutcome outcome, boolean verified) {
        return switch (outcome) {
            case EnqueueOutcome.Stored s -> new EnqueueResult.Accepted(sent.id());
            case EnqueueOutcome.Exists e -> {
                if (!verified) requireSameContent(sent);
                yield new EnqueueResult.Accepted(sent.id());
            }
            case EnqueueOutcome.DuplicateKey d -> new EnqueueResult.Duplicate(d.existingId());
        };
    }

    /**
     * An id the caller set that the store already has is a retry only when the stored task is the one being
     * sent. Compared: task type, queue, the payload when the store still has it (a finished task may have
     * dropped it) and the unique key when the stored task still holds one.
     *
     * @return true when the stored task matched, false when the store has no such task
     * @throws TaskIdReusedException when it differs
     */
    private boolean requireSameContent(TaskRecord sent) {
        var stored = store.getTask(sent.id()).orElse(null);
        if (stored == null) return false;
        if (stored.typeId() != sent.typeId()) throw new TaskIdReusedException(sent.id(), "another task type");
        if (stored.queueId() != sent.queueId()) throw new TaskIdReusedException(sent.id(), "another queue");
        if (stored.encodedPayload() != null && !stored.encodedPayload().equals(sent.encodedPayload())) {
            throw new TaskIdReusedException(sent.id(), "another payload");
        }
        var heldKey = stored.uniqueKey();
        if (heldKey != null && !heldKey.equals(sent.uniqueKey())) {
            throw new TaskIdReusedException(sent.id(), "another unique key");
        }
        return true;
    }

    /**
     * A request with a caller id may be the resend of one that was accepted: its {@code startBy} can have passed
     * since, and a {@code delay} counts from now again. The store then answers that the id exists, also for a
     * task that was removed while its unique key is kept.
     */
    private boolean alreadyStored(TaskRequest<?> request) {
        var id = request.id();
        if (id == null) return false;
        if (store.getTask(id).isPresent()) return true;
        return request.uniqueKey() != null && store.holdsKey(id, request.uniqueKey());
    }

    <T> TaskRecord toRecord(TaskRequest<T> request) {
        return toRecord(request, request.id() != null ? request.id() : ids.nextId());
    }

    private <T> TaskRecord toRecord(TaskRequest<T> request, long id) {
        var type = request.type();
        var now = clock.instant();
        StoredTime.require("at", request.at(), now);
        StoredTime.require("startBy", request.startBy(), now);
        Instant due = request.at();
        if (due == null && request.delay() != null) due = now.plus(request.delay());
        if (request.startBy() != null
                && request.startBy().isBefore(due != null ? due : now)
                && !alreadyStored(request)) {
            throw new TaskValidationException("startBy is before the run time");
        }
        var b = TaskRecord.builder()
                .id(id)
                .revision(IdAllocator.firstRevision())
                .queueId(names.queueId(queues.validQueue(request.queue())))
                .typeId(names.typeId(type.name()))
                .payloadVersion(type.payloadVersion())
                .state(due != null && due.isAfter(now) ? TaskState.SCHEDULED : TaskState.QUEUED)
                .maxAttempts(request.maxAttempts() != null ? request.maxAttempts() : defaultMaxAttempts)
                .createdAt(now)
                .due(due)
                .encodedPayload(codecs.encode(type, request.payload()))
                .account(request.account())
                .headers(validHeaders(request.headers()))
                .uniqueKey(request.uniqueKey())
                .retryPolicy(request.retryPolicy())
                .startBy(request.startBy())
                .maxRunTime(request.maxRunTime());
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
     * Creates or replaces a recurring definition. Re-registering with unchanged timing keeps the stored next
     * fire time while it is no later than the second fire after now (the margin absorbs clock skew between
     * this process and the runner that advanced it); a changed timing, or a stored time beyond that margin
     * such as a parked fixed-delay definition, resets it. A fixed-delay definition first fires one delay from
     * now.
     *
     * @return the next fire time it has after the put: the stored one when it was kept
     */
    public <T> Instant putSchedule(ScheduleDefinition<T> definition) {
        var now = clock.instant();
        Instant next;
        Instant keepUntil;
        if (definition.isFixedDelay()) {
            next = now.plus(definition.fixedDelay());
            keepUntil = next;
        } else {
            Cron.validate(definition.cron());
            var zone = definition.zone();
            next = Cron.next(definition.cron(), now, zone)
                    .orElseThrow(
                            () -> new TaskValidationException("Cron expression never fires: " + definition.cron()));
            keepUntil = Cron.next(definition.cron(), next, zone).orElse(next);
        }
        var type = definition.type();
        var stored = new RecurringDefinition(
                definition.id(),
                names.typeId(type.name()),
                type.payloadVersion(),
                names.queueId(queues.validQueue(definition.queue())),
                definition.account(),
                definition.cron(),
                definition.zone() == null ? null : definition.zone().getId(),
                RecurringDefinition.misfireCode(definition.misfire()),
                RecurringDefinition.misfireMaxFires(definition.misfire()),
                definition.fixedDelay() == null ? null : definition.fixedDelay().toMillis(),
                definition.overlap() == Overlap.SKIP,
                codecs.encode(type, definition.payload()),
                definition.maxAttempts() != null ? definition.maxAttempts() : defaultMaxAttempts,
                definition.maxRunTime() == null ? null : definition.maxRunTime().toMillis(),
                false,
                0);
        var result = store.putRecurring(stored, next, keepUntil);
        var outcome = result.outcome();
        next = result.nextFire();
        var timing = definition.isFixedDelay() ? "every " + definition.fixedDelay() : definition.cron();
        if (outcome == PutRecurringOutcome.UNCHANGED) {
            log.debug("Schedule {} ({}) is registered already", definition.id(), timing);
        } else {
            log.info(
                    "{} schedule {} ({}) next at {}",
                    outcome == PutRecurringOutcome.CREATED ? "Registered" : "Changed",
                    definition.id(),
                    timing,
                    next);
        }
        return next;
    }

    /** @return true when a definition with that id existed */
    public boolean deleteSchedule(String id) {
        StoredText.requirePresent("schedule id", id);
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

    /** The rule for a unique key that is looked up: any text a store can keep. */
    static void requireLookupKey(String key) {
        if (key == null) throw new TaskValidationException("uniqueKey is required");
        StoredText.require("uniqueKey", key);
    }

    /**
     * @return the task that holds the unique key now, open or finished inside its keep window; empty when no task
     *         holds the key
     */
    public Optional<TaskView> taskByKey(String key) {
        requireLookupKey(key);
        return store.taskByKey(key).map(names::view);
    }

    /**
     * @return counts per state by queue name, for every registered queue; a queue without tasks has every count
     *         zero
     */
    public Map<String, StateCounts> counts() {
        var result = new java.util.LinkedHashMap<String, StateCounts>();
        store.counts().forEach((id, c) -> result.put(names.queueName(id), c));
        return result;
    }

    /**
     * Lists tasks in one state, QUEUED and SCHEDULED earliest due first and the other states oldest state change
     * first, paging by a cursor that is a position in the listing.
     *
     * @param queue  a queue name, or null for every queue
     * @param cursor {@link TaskViewPage#next()} of the previous page; null starts at the beginning
     * @param limit  the most tasks one page holds, at least 1
     * @throws TaskValidationException if the cursor is malformed or the limit is below 1
     */
    public TaskViewPage tasks(TaskState state, String queue, String cursor, int limit) {
        var queueId = queue == null ? null : Integer.valueOf(names.queueId(queues.validQueue(queue)));
        var page = store.tasks(state, queueId, cursor, limit);
        var result = new ArrayList<TaskView>();
        for (var r : page.items()) result.add(names.view(r));
        return new TaskViewPage(result, page.next());
    }

    FullStore store() {
        return store;
    }

    Names names() {
        return names;
    }

    Codecs codecs() {
        return codecs;
    }

    IdAllocator ids() {
        return ids;
    }

    QueuesHolder queues() {
        return queues;
    }

    Clock clock() {
        return clock;
    }

    public int defaultMaxAttempts() {
        return defaultMaxAttempts;
    }

    public static final class Builder {
        private FullStore store;
        private Names names;
        private JsonMapper jsonMapper;
        private Clock clock = Clock.systemUTC();
        private int idBlockSize = 64;
        private int defaultMaxAttempts = 10;
        private QueuesHolder queues = QueuesHolder.of(Queues.of(Queues.GENERAL), null);

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
            if (clock == null) throw new TaskConfigurationException("clock cannot be null");
            this.clock = clock;
            return this;
        }

        /** How many ids are reserved per store call; 1 means one call per enqueue. Default 64. */
        public Builder idBlockSize(int size) {
            if (size < 1 || size > 100_000) throw new TaskConfigurationException("idBlockSize must be 1 to 100000");
            this.idBlockSize = size;
            return this;
        }

        /** Attempts for requests that do not set their own. Default 10. */
        public Builder defaultMaxAttempts(int attempts) {
            if (attempts < 1 || attempts > TaskRequest.MAX_ATTEMPTS) {
                throw new TaskConfigurationException("defaultMaxAttempts must be 1 to " + TaskRequest.MAX_ATTEMPTS);
            }
            this.defaultMaxAttempts = attempts;
            return this;
        }

        /**
         * The queues tasks may be enqueued to and the one used when a request names none; give the runners of
         * the application the same value. Default: the one queue {@link Queues#GENERAL}.
         */
        public Builder queues(Queues queues) {
            if (queues == null) throw new TaskConfigurationException("queues cannot be null");
            this.queues = QueuesHolder.of(queues, null);
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
            if (store == null) throw new TaskConfigurationException("store is required");
            store.init();
            return new TaskClient(this);
        }
    }
}
