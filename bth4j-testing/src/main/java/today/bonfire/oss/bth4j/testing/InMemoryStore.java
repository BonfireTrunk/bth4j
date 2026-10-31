package today.bonfire.oss.bth4j.testing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Stream;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.LeaseStatus;
import today.bonfire.oss.bth4j.spi.PromoteResult;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.RecurringSchedule;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.StoreInfo;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/**
 * A {@link FullStore} that keeps everything in memory, for tests of applications that use bth4j. Every
 * operation runs under one lock; all store time comes from the {@link Clock} given at construction.
 */
public final class InMemoryStore implements FullStore {

    private static final int MAX_FAILURES = 20;
    private static final Comparator<TaskRecord> BY_DUE =
            Comparator.comparing(TaskRecord::due).thenComparingLong(TaskRecord::id);

    private record Recurring(RecurringDefinition definition, Instant nextFire) {}

    private final Clock clock;
    private final int registryBound;
    private final ReentrantLock lock = new ReentrantLock();

    private final Map<Long, TaskRecord> tasks = new HashMap<>();
    private final Map<String, Long> keyHolders = new HashMap<>();
    private final Map<String, Recurring> recurring = new HashMap<>();
    private final Map<String, Integer> queueIds = new LinkedHashMap<>();
    private final Map<String, Integer> eventIds = new LinkedHashMap<>();
    private final Set<Integer> pausedQueues = new HashSet<>();
    private final Map<Integer, WorkerInfo> workers = new TreeMap<>();
    private long nextTaskId = 1;
    private int nextWorkerId = 1;
    private long recurringRevision;

    public InMemoryStore() {
        this(Clock.systemUTC());
    }

    public InMemoryStore(Clock clock) {
        this(clock, 32767);
    }

    InMemoryStore(Clock clock, int registryBound) {
        this.clock = clock;
        this.registryBound = registryBound;
    }

    @Override
    public StoreInfo info() {
        return new StoreInfo("memory", StoreInfo.SCHEMA_VERSION);
    }

    @Override
    public void init() {}

    @Override
    public long allocateIds(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be positive: " + count);
        }
        lock.lock();
        try {
            long first = nextTaskId;
            nextTaskId += count;
            return first;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int allocateWorkerId(String label) {
        lock.lock();
        try {
            int id = nextWorkerId++;
            Instant now = clock.instant();
            workers.put(id, new WorkerInfo(id, label, now, now, 0));
            return id;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int queueId(String name) {
        return register(queueIds, name, "queue");
    }

    @Override
    public int eventId(String name) {
        return register(eventIds, name, "event");
    }

    private int register(Map<String, Integer> registry, String name, String kind) {
        lock.lock();
        try {
            Integer existing = registry.get(name);
            if (existing != null) {
                return existing;
            }
            if (registry.size() >= registryBound) {
                throw new TaskStoreException("Too many " + kind + " names registered, the limit is " + registryBound);
            }
            int id = registry.size() + 1;
            registry.put(name, id);
            return id;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<Integer, String> queueNames() {
        return names(queueIds);
    }

    @Override
    public Map<Integer, String> eventNames() {
        return names(eventIds);
    }

    private Map<Integer, String> names(Map<String, Integer> registry) {
        lock.lock();
        try {
            Map<Integer, String> result = new LinkedHashMap<>();
            registry.forEach((name, id) -> result.put(id, name));
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public EnqueueOutcome enqueue(TaskRecord task) {
        lock.lock();
        try {
            return enqueueLocked(task, null);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<EnqueueOutcome> enqueueAll(List<TaskRecord> list) {
        lock.lock();
        try {
            List<EnqueueOutcome> result = new ArrayList<>(list.size());
            for (TaskRecord task : list) {
                result.add(enqueueLocked(task, null));
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    /** @param forcedDue when not null the task is SCHEDULED for that instant whatever the clock says */
    private EnqueueOutcome enqueueLocked(TaskRecord task, Instant forcedDue) {
        Instant now = clock.instant();
        if (tasks.containsKey(task.id())) {
            return EnqueueOutcome.EXISTS;
        }
        String key = task.uniqueKey();
        if (key != null) {
            Long holderId = keyHolders.get(key);
            if (holderId != null) {
                TaskRecord holder = tasks.get(holderId);
                if (holder != null && keyHeld(holder, now)) {
                    return new EnqueueOutcome.DuplicateKey(holderId);
                }
                releaseKey(holderId);
            }
        }
        Instant due = forcedDue != null ? forcedDue : task.due() != null ? task.due() : now;
        TaskState state = forcedDue != null || due.isAfter(now) ? TaskState.SCHEDULED : TaskState.QUEUED;
        tasks.put(
                task.id(),
                task.toBuilder().state(state).due(due).stateChanged(now).build());
        if (key != null) {
            keyHolders.put(key, task.id());
        }
        return EnqueueOutcome.STORED;
    }

    private static boolean keyHeld(TaskRecord holder, Instant now) {
        return !holder.state().isTerminal()
                || (holder.keyUntil() != null && holder.keyUntil().isAfter(now));
    }

    private void releaseKey(long id) {
        TaskRecord t = tasks.get(id);
        if (t == null || t.uniqueKey() == null) {
            return;
        }
        keyHolders.remove(t.uniqueKey(), id);
        tasks.put(id, t.toBuilder().uniqueKey(null).keyUntil(null).build());
    }

    @Override
    public List<TaskRecord> claim(ClaimRequest request) {
        lock.lock();
        try {
            Instant now = clock.instant();
            Set<Integer> known = request.knownEventIds();
            List<TaskRecord> claimed = new ArrayList<>();
            for (int queueId : request.queueIds()) {
                if (pausedQueues.contains(queueId)) {
                    continue;
                }
                int missing = request.count() - claimed.size();
                if (missing <= 0) {
                    break;
                }
                List<TaskRecord> candidates = tasks.values().stream()
                        .filter(t -> t.state() == TaskState.QUEUED && t.queueId() == queueId)
                        .filter(t -> t.startBy() == null || t.startBy().isAfter(now))
                        .filter(t -> known == null || known.contains(t.eventId()))
                        .sorted(BY_DUE)
                        .limit(missing)
                        .toList();
                for (TaskRecord t : candidates) {
                    TaskRecord leased = t.toBuilder()
                            .state(TaskState.LEASED)
                            .attempts(t.attempts() + 1)
                            .revision(t.revision() + 1)
                            .claimedAt(now)
                            .stateChanged(now)
                            .leaseUntil(now.plus(request.lease()))
                            .workerId(request.workerId())
                            .build();
                    tasks.put(t.id(), leased);
                    claimed.add(leased);
                }
            }
            return claimed;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<LeaseStatus> renew(List<LeaseRef> leases, Duration lease) {
        lock.lock();
        try {
            Instant until = clock.instant().plus(lease);
            List<LeaseStatus> result = new ArrayList<>(leases.size());
            for (LeaseRef ref : leases) {
                TaskRecord t = tasks.get(ref.id());
                if (t == null) {
                    result.add(new LeaseStatus(ref.id(), false, false));
                } else if (!fenced(t, ref.revision())) {
                    result.add(new LeaseStatus(ref.id(), false, t.cancelRequestedAt() != null));
                } else {
                    tasks.put(t.id(), t.toBuilder().leaseUntil(until).build());
                    result.add(new LeaseStatus(ref.id(), true, t.cancelRequestedAt() != null));
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    private static boolean fenced(TaskRecord t, int revision) {
        return t.state() == TaskState.LEASED && t.revision() == revision;
    }

    @Override
    public boolean ack(long id, int revision, boolean keep, boolean keepPayload) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || !fenced(t, revision)) {
                return false;
            }
            Instant now = clock.instant();
            if (!keep && t.keyKeep() == null) {
                remove(t);
                return true;
            }
            TaskRecord.Builder b = t.toBuilder()
                    .state(TaskState.SUCCEEDED)
                    .revision(t.revision() + 1)
                    .stateChanged(now)
                    .leaseUntil(null);
            if (!keep || !keepPayload) {
                b.data(null);
            }
            finish(t, b, now);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Applies the unique key rule of a task that just reached a terminal state, then stores it. */
    private void finish(TaskRecord before, TaskRecord.Builder after, Instant now) {
        if (before.uniqueKey() != null) {
            if (before.keyKeep() == null) {
                keyHolders.remove(before.uniqueKey(), before.id());
                after.uniqueKey(null).keyUntil(null);
            } else {
                after.keyUntil(now.plus(before.keyKeep()));
            }
        }
        tasks.put(before.id(), after.build());
    }

    private void remove(TaskRecord t) {
        tasks.remove(t.id());
        if (t.uniqueKey() != null) {
            keyHolders.remove(t.uniqueKey(), t.id());
        }
    }

    @Override
    public FailOutcome fail(long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || !fenced(t, revision)) {
                return FailOutcome.LOST;
            }
            return failLocked(t, failure, retryAt, keepDead);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public FailOutcome failExpired(long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || !fenced(t, revision) || t.leaseUntil().isAfter(clock.instant())) {
                return FailOutcome.LOST;
            }
            return failLocked(t, failure, retryAt, keepDead);
        } finally {
            lock.unlock();
        }
    }

    private FailOutcome failLocked(TaskRecord t, FailureInfo failure, Instant retryAt, boolean keepDead) {
        Instant now = clock.instant();
        List<FailureInfo> failures = new ArrayList<>();
        if (t.failures() != null) {
            failures.addAll(t.failures());
        }
        failures.add(failure);
        while (failures.size() > MAX_FAILURES) {
            failures.remove(0);
        }
        TaskRecord.Builder b = t.toBuilder()
                .failures(failures)
                .revision(t.revision() + 1)
                .stateChanged(now)
                .leaseUntil(null);
        if (retryAt != null && t.attempts() < t.maxAttempts()) {
            tasks.put(
                    t.id(),
                    b.state(TaskState.SCHEDULED).due(retryAt).workerId(null).build());
            return FailOutcome.RETRY;
        }
        if (!keepDead && t.keyKeep() == null) {
            remove(t);
            return FailOutcome.DEAD;
        }
        finish(t, b.state(TaskState.DEAD), now);
        return FailOutcome.DEAD;
    }

    @Override
    public boolean reschedule(long id, int revision, Instant due) {
        return backToWaiting(id, revision, b -> b.state(TaskState.SCHEDULED).due(due));
    }

    @Override
    public boolean unclaim(long id, int revision) {
        return backToWaiting(id, revision, b -> b.state(TaskState.QUEUED));
    }

    private boolean backToWaiting(long id, int revision, java.util.function.Consumer<TaskRecord.Builder> target) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || !fenced(t, revision)) {
                return false;
            }
            TaskRecord.Builder b = t.toBuilder()
                    .attempts(t.attempts() - 1)
                    .revision(t.revision() + 1)
                    .stateChanged(clock.instant())
                    .leaseUntil(null)
                    .workerId(null);
            target.accept(b);
            tasks.put(id, b.build());
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean saveSteps(long id, int revision, Map<String, StepResult> steps) {
        return fencedUpdate(id, revision, b -> b.steps(steps));
    }

    @Override
    public boolean progress(long id, int revision, String progress) {
        return fencedUpdate(id, revision, b -> b.progress(progress));
    }

    private boolean fencedUpdate(long id, int revision, java.util.function.Consumer<TaskRecord.Builder> change) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || !fenced(t, revision)) {
                return false;
            }
            TaskRecord.Builder b = t.toBuilder();
            change.accept(b);
            tasks.put(id, b.build());
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PromoteResult promote(int limit) {
        lock.lock();
        try {
            Instant now = clock.instant();
            List<TaskRecord> due = tasks.values().stream()
                    .filter(t -> t.state() == TaskState.SCHEDULED && !t.due().isAfter(now))
                    .sorted(BY_DUE)
                    .limit(limit)
                    .toList();
            for (TaskRecord t : due) {
                tasks.put(
                        t.id(),
                        t.toBuilder()
                                .state(TaskState.QUEUED)
                                .revision(t.revision() + 1)
                                .stateChanged(now)
                                .build());
            }
            return new PromoteResult(due.size(), nextDueLocked());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<ExpiredLease> reclaim(int limit) {
        lock.lock();
        try {
            Instant now = clock.instant();
            return tasks.values().stream()
                    .filter(t ->
                            t.state() == TaskState.LEASED && !t.leaseUntil().isAfter(now))
                    .sorted(Comparator.comparing(TaskRecord::leaseUntil).thenComparingLong(TaskRecord::id))
                    .limit(limit)
                    .map(t -> new ExpiredLease(
                            t.id(),
                            t.revision(),
                            t.attempts(),
                            t.maxAttempts(),
                            t.eventId(),
                            t.cancelRequestedAt() != null))
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long purge(TaskState state, Duration retention, int limit) {
        lock.lock();
        try {
            Instant now = clock.instant();
            Instant cutoff = now.minus(retention);
            List<TaskRecord> old = tasks.values().stream()
                    .filter(t -> t.state() == state && t.stateChanged().isBefore(cutoff))
                    .filter(t -> t.keyUntil() == null || !t.keyUntil().isAfter(now))
                    .sorted(Comparator.comparing(TaskRecord::stateChanged).thenComparingLong(TaskRecord::id))
                    .limit(limit)
                    .toList();
            old.forEach(this::remove);
            return old.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int releaseKeys(int limit) {
        lock.lock();
        try {
            Instant now = clock.instant();
            List<Long> expired = tasks.values().stream()
                    .filter(t -> t.state().isTerminal()
                            && t.keyUntil() != null
                            && !t.keyUntil().isAfter(now))
                    .sorted(Comparator.comparing(TaskRecord::keyUntil).thenComparingLong(TaskRecord::id))
                    .limit(limit)
                    .map(TaskRecord::id)
                    .toList();
            expired.forEach(this::releaseKey);
            return expired.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Instant nextDue() {
        lock.lock();
        try {
            return nextDueLocked();
        } finally {
            lock.unlock();
        }
    }

    private Instant nextDueLocked() {
        return tasks.values().stream()
                .filter(t -> t.state() == TaskState.SCHEDULED)
                .map(TaskRecord::due)
                .min(Comparator.naturalOrder())
                .orElse(null);
    }

    @Override
    public void putRecurring(RecurringDefinition definition, Instant nextFire) {
        lock.lock();
        try {
            Recurring old = recurring.get(definition.scheduleId());
            RecurringDefinition stored = new RecurringDefinition(
                    definition.scheduleId(),
                    definition.eventId(),
                    definition.payloadVersion(),
                    definition.queueId(),
                    definition.account(),
                    definition.cron(),
                    definition.zone(),
                    definition.misfire(),
                    definition.catchUpMax(),
                    definition.fixedDelayMs(),
                    definition.overlapSkip(),
                    definition.data(),
                    definition.maxAttempts(),
                    old != null ? old.definition().paused() : definition.paused(),
                    ++recurringRevision);
            Instant fire = old != null
                            && Objects.equals(old.definition().cron(), definition.cron())
                            && Objects.equals(old.definition().zone(), definition.zone())
                            && Objects.equals(old.definition().fixedDelayMs(), definition.fixedDelayMs())
                    ? old.nextFire()
                    : nextFire;
            recurring.put(definition.scheduleId(), new Recurring(stored, fire));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean deleteRecurring(String scheduleId) {
        lock.lock();
        try {
            return recurring.remove(scheduleId) != null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<String, Instant> recurringSchedules() {
        lock.lock();
        try {
            Map<String, Instant> result = new TreeMap<>();
            recurring.forEach((id, r) -> result.put(id, r.nextFire()));
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<DueSchedule> dueRecurring(Instant upTo, int limit) {
        lock.lock();
        try {
            return recurring.entrySet().stream()
                    .filter(e -> !e.getValue().definition().paused())
                    .filter(e -> !e.getValue().nextFire().isAfter(upTo))
                    .sorted(Comparator.comparing((Map.Entry<String, Recurring> e) ->
                                    e.getValue().nextFire())
                            .thenComparing(Map.Entry::getKey))
                    .limit(limit)
                    .map(e -> new DueSchedule(
                            e.getKey(), e.getValue().nextFire(), e.getValue().definition()))
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord occurrence) {
        lock.lock();
        try {
            Recurring current = recurring.get(due.scheduleId());
            if (current == null
                    || current.definition().paused()
                    || current.definition().revision() != due.definition().revision()
                    || !current.nextFire().equals(due.fireAt())) {
                return false;
            }
            EnqueueOutcome outcome = enqueueLocked(occurrence, due.fireAt());
            if (outcome == EnqueueOutcome.EXISTS) {
                return false;
            }
            recurring.put(due.scheduleId(), new Recurring(current.definition(), nextFire));
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<TaskRecord> getTask(long id) {
        lock.lock();
        try {
            return Optional.ofNullable(tasks.get(id));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<Integer, StateCounts> counts() {
        lock.lock();
        try {
            Map<Integer, long[]> raw = new TreeMap<>();
            for (TaskRecord t : tasks.values()) {
                raw.computeIfAbsent(t.queueId(), q -> new long[TaskState.values().length])[
                        t.state().code()]++;
            }
            Map<Integer, StateCounts> result = new TreeMap<>();
            raw.forEach((q, c) -> result.put(q, new StateCounts(c[0], c[1], c[2], c[3], c[4], c[5], c[6])));
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<TaskRecord> tasks(TaskState state, Integer queueId, long afterId, int limit) {
        lock.lock();
        try {
            Comparator<TaskRecord> order =
                    Comparator.comparing(TaskRecord::stateChanged).thenComparingLong(TaskRecord::id);
            TaskRecord cursor = afterId == 0 ? null : tasks.get(afterId);
            Predicate<TaskRecord> afterCursor = afterId == 0
                    ? t -> true
                    : cursor != null ? t -> order.compare(t, cursor) > 0 : t -> t.id() > afterId;
            Stream<TaskRecord> stream = tasks.values().stream()
                    .filter(t -> t.state() == state)
                    .filter(t -> queueId == null || t.queueId() == queueId)
                    .filter(afterCursor)
                    .sorted(order)
                    .limit(limit);
            return stream.toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean finishCancelled(long id, int revision, String reason, boolean keepPayload) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || !fenced(t, revision)) {
                return false;
            }
            Instant now = clock.instant();
            TaskRecord.Builder b = t.toBuilder()
                    .state(TaskState.CANCELLED)
                    .revision(t.revision() + 1)
                    .stateChanged(now)
                    .cancelReason(reason)
                    .cancelRequestedAt(t.cancelRequestedAt() != null ? t.cancelRequestedAt() : now)
                    .leaseUntil(null);
            if (!keepPayload) {
                b.data(null);
            }
            finish(t, b, now);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Long> expire(int limit) {
        lock.lock();
        try {
            Instant now = clock.instant();
            List<TaskRecord> overdue = tasks.values().stream()
                    .filter(t -> (t.state() == TaskState.QUEUED || t.state() == TaskState.SCHEDULED)
                            && t.startBy() != null
                            && !t.startBy().isAfter(now))
                    .sorted(Comparator.comparing(TaskRecord::startBy).thenComparingLong(TaskRecord::id))
                    .limit(limit)
                    .toList();
            for (TaskRecord t : overdue) {
                finish(
                        t,
                        t.toBuilder()
                                .state(TaskState.EXPIRED)
                                .revision(t.revision() + 1)
                                .stateChanged(now),
                        now);
            }
            return overdue.stream().map(TaskRecord::id).toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int purgeWorkers(Duration staleAfter) {
        lock.lock();
        try {
            Instant cutoff = clock.instant().minus(staleAfter);
            int before = workers.size();
            workers.values().removeIf(w -> w.lastSeen().isBefore(cutoff));
            return before - workers.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CancelOutcome cancel(long id, int revision, String reason) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || t.revision() != revision) {
                return CancelOutcome.STALE;
            }
            Instant now = clock.instant();
            if (t.state() == TaskState.QUEUED || t.state() == TaskState.SCHEDULED) {
                finish(
                        t,
                        t.toBuilder()
                                .state(TaskState.CANCELLED)
                                .revision(t.revision() + 1)
                                .stateChanged(now)
                                .cancelReason(reason)
                                .cancelRequestedAt(now),
                        now);
                return CancelOutcome.CANCELLED;
            }
            if (t.state() == TaskState.LEASED) {
                tasks.put(
                        id,
                        t.toBuilder()
                                .cancelReason(reason)
                                .cancelRequestedAt(now)
                                .build());
                return CancelOutcome.REQUESTED;
            }
            return CancelOutcome.STALE;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean retry(long id, int revision) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null
                    || t.revision() != revision
                    || (t.state() != TaskState.DEAD
                            && t.state() != TaskState.CANCELLED
                            && t.state() != TaskState.EXPIRED)) {
                return false;
            }
            Instant now = clock.instant();
            if (t.uniqueKey() != null) {
                keyHolders.remove(t.uniqueKey(), id);
            }
            tasks.put(
                    id,
                    t.toBuilder()
                            .state(TaskState.QUEUED)
                            .due(now)
                            .stateChanged(now)
                            .attempts(0)
                            .revision(t.revision() + 1)
                            .cancelRequestedAt(null)
                            .cancelReason(null)
                            .workerId(null)
                            .leaseUntil(null)
                            .keyKeep(null)
                            .keyUntil(null)
                            .uniqueKey(null)
                            .startBy(null)
                            .build());
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean delete(long id, int revision) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || t.revision() != revision || t.state() == TaskState.LEASED) {
                return false;
            }
            remove(t);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void pauseQueue(int queueId, boolean paused) {
        lock.lock();
        try {
            if (paused) {
                pausedQueues.add(queueId);
            } else {
                pausedQueues.remove(queueId);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<QueueInfo> queues() {
        lock.lock();
        try {
            return queueIds.entrySet().stream()
                    .map(e -> new QueueInfo(e.getValue(), e.getKey(), pausedQueues.contains(e.getValue())))
                    .sorted(Comparator.comparingInt(QueueInfo::id))
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void heartbeat(int workerId, int running) {
        lock.lock();
        try {
            WorkerInfo w = workers.get(workerId);
            if (w != null) {
                workers.put(workerId, new WorkerInfo(w.id(), w.label(), w.startedAt(), clock.instant(), running));
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<WorkerInfo> workers() {
        lock.lock();
        try {
            return List.copyOf(workers.values());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<RecurringSchedule> recurringDefinitions() {
        lock.lock();
        try {
            return recurring.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> new RecurringSchedule(
                            e.getValue().definition(), e.getValue().nextFire()))
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean pauseRecurring(String scheduleId, boolean paused) {
        lock.lock();
        try {
            Recurring r = recurring.get(scheduleId);
            if (r == null) {
                return false;
            }
            recurring.put(scheduleId, new Recurring(r.definition().withPaused(paused), r.nextFire()));
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean setNextFire(String scheduleId, Instant nextFire) {
        lock.lock();
        try {
            Recurring r = recurring.get(scheduleId);
            if (r == null) {
                return false;
            }
            recurring.put(scheduleId, new Recurring(r.definition(), nextFire));
            return true;
        } finally {
            lock.unlock();
        }
    }
}
