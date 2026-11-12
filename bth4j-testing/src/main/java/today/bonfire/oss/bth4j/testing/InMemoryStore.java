package today.bonfire.oss.bth4j.testing;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import java.util.function.Function;
import today.bonfire.oss.bth4j.CancelOutcome;
import today.bonfire.oss.bth4j.DeleteOutcome;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.PutScheduleOutcome;
import today.bonfire.oss.bth4j.PutScheduleResult;
import today.bonfire.oss.bth4j.QueueInfo;
import today.bonfire.oss.bth4j.RetryOutcome;
import today.bonfire.oss.bth4j.StateCounts;
import today.bonfire.oss.bth4j.StepResult;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.ExpiredTask;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.LeaseStatus;
import today.bonfire.oss.bth4j.spi.PromoteResult;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.RunnerRecord;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.ScheduleEntry;
import today.bonfire.oss.bth4j.spi.ScheduleStore;
import today.bonfire.oss.bth4j.spi.StoreArguments;
import today.bonfire.oss.bth4j.spi.StoreInfo;
import today.bonfire.oss.bth4j.spi.StoredSchedule;
import today.bonfire.oss.bth4j.spi.TaskPage;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * A {@link FullStore} that keeps everything in memory, for tests of applications that use bth4j. Every
 * operation runs under one lock; all store time comes from the {@link Clock} given at construction.
 */
public final class InMemoryStore implements FullStore {

    private static final int MAX_FAILURES = 20;
    private static final Comparator<TaskRecord> BY_DUE =
            Comparator.comparing(TaskRecord::dueAt).thenComparingLong(TaskRecord::id);

    /**
     * @param occurrence the id of the in-flight fixed-delay occurrence, or null
     * @param triggered whether an operator set this next fire time
     */
    private record Recurring(StoredSchedule definition, Instant nextFireAt, Long occurrence, boolean triggered) {
        Recurring {
            nextFireAt = millis(nextFireAt);
        }

        /** A next fire time that is written anew is not a triggered one. */
        Recurring(StoredSchedule definition, Instant nextFire, Long occurrence) {
            this(definition, nextFire, occurrence, false);
        }

        Recurring(StoredSchedule definition, Instant nextFire) {
            this(definition, nextFire, null);
        }

        Recurring withNext(Instant next) {
            return new Recurring(definition, next, occurrence);
        }

        Recurring withDefinition(StoredSchedule changed) {
            return new Recurring(changed, nextFireAt, occurrence, triggered);
        }
    }

    private final Clock clock;
    private final int registryBound;
    private final ReentrantLock lock = new ReentrantLock();

    private final Map<Long, TaskRecord> tasks = new HashMap<>();
    private final Map<String, Long> keyHolders = new HashMap<>();
    /** Keys of removed tasks whose keep window is still open; the task is gone, its key stays taken. */
    private final Map<String, KeptKey> keptKeys = new HashMap<>();

    private final Map<String, Recurring> recurring = new HashMap<>();
    private final Map<String, Integer> queueIds = new LinkedHashMap<>();
    private final Map<String, Integer> typeIds = new LinkedHashMap<>();
    private final Set<Integer> pausedQueues = new HashSet<>();
    private final Map<Integer, RunnerRecord> runners = new TreeMap<>();
    private long nextTaskId = 1;
    private int nextRunnerId = 1;
    private long recurringRevision;

    public InMemoryStore() {
        this(Clock.systemUTC());
    }

    public InMemoryStore(Clock clock) {
        this(clock, 32767);
    }

    InMemoryStore(Clock clock, int registryBound) {
        this.clock = Clock.tick(clock, Duration.ofMillis(1));
        this.registryBound = registryBound;
    }

    @Override
    public StoreInfo info() {
        return new StoreInfo("memory", 1);
    }

    @Override
    public void init() {}

    @Override
    public long idHighWaterMark() {
        lock.lock();
        try {
            return nextTaskId;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long allocateIds(int count) {
        StoreArguments.requireCount(count);
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
    public int allocateRunnerId(String label, Set<Integer> queueIds, Set<Integer> typeIds) {
        StoreArguments.requireLabel(label);
        lock.lock();
        try {
            int id = nextRunnerId++;
            Instant now = clock.instant();
            runners.put(id, new RunnerRecord(id, label, now, now, 0, true, queueIds, typeIds));
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
    public int typeId(String name) {
        return register(typeIds, name, "type");
    }

    private int register(Map<String, Integer> registry, String name, String kind) {
        StoreArguments.requireName(kind, name);
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
    public Map<Integer, String> typeNames() {
        return names(typeIds);
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
        StoreArguments.requireKey(task.uniqueKey());
        lock.lock();
        try {
            return enqueueLocked(task, null);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<EnqueueOutcome> enqueueAll(List<TaskRecord> list) {
        list.forEach(task -> StoreArguments.requireKey(task.uniqueKey()));
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
            Long keptBy = keptKeyHolder(key, now);
            if (keptBy != null) {
                // the removed task itself, sent again: it was stored once, which is what EXISTS says
                return keptBy == task.id() ? EnqueueOutcome.EXISTS : new EnqueueOutcome.DuplicateKey(keptBy);
            }
        }
        Instant due = millis(forcedDue != null ? forcedDue : task.dueAt() != null ? task.dueAt() : now);
        TaskState state = forcedDue != null || due.isAfter(now) ? TaskState.SCHEDULED : TaskState.QUEUED;
        save(task.asNew().toBuilder()
                .createdAt(task.createdAt() != null ? task.createdAt() : now)
                .state(state)
                .dueAt(due)
                .stateChangedAt(now)
                .build());
        if (key != null) {
            keyHolders.put(key, task.id());
        }
        return EnqueueOutcome.STORED;
    }

    /** Stores a task with its times in whole milliseconds, as the other stores keep them, and returns it. */
    private TaskRecord save(TaskRecord t) {
        TaskRecord stored = t.toBuilder()
                .createdAt(millis(t.createdAt()))
                .dueAt(millis(t.dueAt()))
                .stateChangedAt(millis(t.stateChangedAt()))
                .leaseUntil(millis(t.leaseUntil()))
                .claimedAt(millis(t.claimedAt()))
                .keyUntil(millis(t.keyUntil()))
                .startBy(millis(t.startBy()))
                .cancelRequestedAt(millis(t.cancelRequestedAt()))
                .keyKeep(millis(t.keyKeep()))
                .maxRunTime(millis(t.maxRunTime()))
                .build();
        tasks.put(stored.id(), stored);
        return stored;
    }

    private static Instant millis(Instant t) {
        return t == null ? null : t.truncatedTo(ChronoUnit.MILLIS);
    }

    private static Duration millis(Duration d) {
        return d == null ? null : Duration.ofMillis(d.toMillis());
    }

    private static boolean keyHeld(TaskRecord holder, Instant now) {
        return !holder.state().isTerminal()
                || (holder.keyUntil() != null && holder.keyUntil().isAfter(now));
    }

    /** The id of the removed task that still keeps this key taken; a passed window is dropped here. */
    private Long keptKeyHolder(String key, Instant now) {
        KeptKey kept = keptKeys.get(key);
        if (kept == null) {
            return null;
        }
        if (kept.until().isAfter(now)) {
            return kept.id();
        }
        keptKeys.remove(key);
        return null;
    }

    private static TaskStoreException keyHeldByAnother(long id, long holderId) {
        return new TaskStoreException("Cannot retry task " + id + ": its unique key is held by task " + holderId);
    }

    private void releaseKey(long id) {
        TaskRecord t = tasks.get(id);
        if (t == null || t.uniqueKey() == null) {
            return;
        }
        keyHolders.remove(t.uniqueKey(), id);
        save(t.toBuilder().uniqueKey(null).keyUntil(null).build());
    }

    @Override
    public List<TaskRecord> claim(ClaimRequest request) {
        lock.lock();
        try {
            Instant now = clock.instant();
            List<TaskRecord> claimed = new ArrayList<>();
            var got = new int[request.grants().size()];
            var filled = new boolean[got.length];
            for (int i = 0; i < got.length && claimed.size() < request.count(); i++) {
                var grant = request.grants().get(i);
                if (pausedQueues.contains(grant.queueId()) || grant.share() == 0) continue;
                int take = Math.min(grant.share(), request.count() - claimed.size());
                got[i] = claimQueue(request, grant, take, now, claimed);
                filled[i] = got[i] == take && take > 0;
            }
            for (int i = 0; i < got.length && claimed.size() < request.count(); i++) {
                if (!filled[i]) continue;
                var grant = request.grants().get(i);
                int take = Math.min(request.count() - claimed.size(), grant.cap() - got[i]);
                claimQueue(request, grant, take, now, claimed);
            }
            return claimed;
        } finally {
            lock.unlock();
        }
    }

    private int claimQueue(ClaimRequest request, QueueGrant grant, int take, Instant now, List<TaskRecord> claimed) {
        var known = request.knownTypeIds();
        List<TaskRecord> candidates = tasks.values().stream()
                .filter(t -> t.state() == TaskState.QUEUED && t.queueId() == grant.queueId())
                .filter(t -> t.startBy() == null || t.startBy().isAfter(now))
                .filter(t -> known == null || known.contains(t.typeId()))
                .sorted(BY_DUE)
                .limit(take)
                .toList();
        for (TaskRecord t : candidates) {
            TaskRecord leased = t.toBuilder()
                    .state(TaskState.LEASED)
                    .attempts(t.attempts() + 1)
                    .revision(t.revision() + 1)
                    .claimedAt(now)
                    .stateChangedAt(now)
                    .leaseUntil(now.plus(request.lease()))
                    .runnerId(request.runnerId())
                    .build();
            claimed.add(save(leased));
        }
        return candidates.size();
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
                } else if (!fenced(t, ref.revision()) || !t.leaseUntil().isAfter(clock.instant())) {
                    result.add(new LeaseStatus(ref.id(), false, false));
                } else {
                    save(t.toBuilder().leaseUntil(until).build());
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
    public boolean ack(long id, int revision, boolean keep, boolean keepPayload, ScheduleEffect effect) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || !fenced(t, revision)) {
                return false;
            }
            Instant now = clock.instant();
            applyEffect(effect);
            if (!keep) {
                removeFinished(t, now);
                return true;
            }
            TaskRecord.Builder b = t.toBuilder()
                    .state(TaskState.SUCCEEDED)
                    .revision(t.revision() + 1)
                    .stateChangedAt(now)
                    .leaseUntil(null);
            if (!keep || !keepPayload) {
                b.encodedPayload(null);
            }
            finish(t, b, now);
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void applyEffect(ScheduleEffect effect) {
        if (effect == null) return;
        Recurring r = recurring.get(effect.scheduleId());
        if (r == null || r.occurrence() == null || r.occurrence() != effect.occurrenceId()) return;
        if (effect.isPause()) {
            recurring.put(
                    effect.scheduleId(),
                    new Recurring(r.definition().withPaused(true), r.nextFireAt(), null, r.triggered()));
        } else {
            recurring.put(effect.scheduleId(), new Recurring(r.definition(), effect.nextFireAt(), null));
        }
        effect.markApplied();
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
        save(after.build());
    }

    private void remove(TaskRecord t) {
        tasks.remove(t.id());
        if (t.uniqueKey() != null) {
            keyHolders.remove(t.uniqueKey(), t.id());
        }
    }

    /** Removes a task that just ended; a key with a keep window stays taken by its id until the window ends. */
    private void removeFinished(TaskRecord t, Instant now) {
        removeKeepingKey(t, t.uniqueKey() != null && t.keyKeep() != null ? now.plus(t.keyKeep()) : null, now);
    }

    private void removeKeepingKey(TaskRecord t, Instant keyUntil, Instant now) {
        boolean held = t.uniqueKey() != null && Objects.equals(keyHolders.get(t.uniqueKey()), t.id());
        remove(t);
        if (held && keyUntil != null && keyUntil.isAfter(now)) {
            keptKeys.put(t.uniqueKey(), new KeptKey(t.id(), keyUntil));
        }
    }

    @Override
    public FailOutcome fail(
            long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead, ScheduleEffect effect) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || !fenced(t, revision)) {
                return FailOutcome.LOST;
            }
            return failLocked(t, failure, retryAt, keepDead, effect);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public FailOutcome failExpired(
            long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead, ScheduleEffect effect) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null || !fenced(t, revision) || t.leaseUntil().isAfter(clock.instant())) {
                return FailOutcome.LOST;
            }
            return failLocked(t, failure, retryAt, keepDead, effect);
        } finally {
            lock.unlock();
        }
    }

    private FailOutcome failLocked(
            TaskRecord t, FailureInfo failure, Instant retryAt, boolean keepDead, ScheduleEffect effect) {
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
                .stateChangedAt(now)
                .leaseUntil(null);
        if (retryAt != null && t.attempts() < t.maxAttempts()) {
            save(b.state(TaskState.SCHEDULED).dueAt(retryAt).runnerId(null).build());
            return FailOutcome.RETRY;
        }
        applyEffect(effect);
        if (!keepDead) {
            removeFinished(t, now);
            return FailOutcome.DEAD;
        }
        finish(t, b.state(TaskState.DEAD), now);
        return FailOutcome.DEAD;
    }

    @Override
    public boolean reschedule(long id, int revision, Instant due) {
        StoreArguments.requireDue(due);
        return backToWaiting(id, revision, b -> b.state(TaskState.SCHEDULED).dueAt(due));
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
                    .stateChangedAt(clock.instant())
                    .leaseUntil(null)
                    .runnerId(null);
            target.accept(b);
            save(b.build());
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
            save(b.build());
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PromoteResult promote(int limit) {
        StoreArguments.requireLimit(limit);
        lock.lock();
        try {
            Instant now = clock.instant();
            List<TaskRecord> due = tasks.values().stream()
                    .filter(t -> t.state() == TaskState.SCHEDULED && !t.dueAt().isAfter(now))
                    .sorted(BY_DUE)
                    .limit(limit)
                    .toList();
            for (TaskRecord t : due) {
                save(t.toBuilder()
                        .state(TaskState.QUEUED)
                        .revision(t.revision() + 1)
                        .stateChangedAt(now)
                        .build());
            }
            return new PromoteResult(due.size(), nextDueLocked());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<ExpiredLease> reclaim(int limit) {
        StoreArguments.requireLimit(limit);
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
                            t.queueId(),
                            t.typeId(),
                            t.cancelRequestedAt() != null))
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long purge(TaskState state, Duration retention, int limit) {
        StoreArguments.requireTerminal(state);
        StoreArguments.requireLimit(limit);
        lock.lock();
        try {
            Instant now = clock.instant();
            Instant cutoff = now.minus(millis(retention));
            List<TaskRecord> old = tasks.values().stream()
                    .filter(t -> t.state() == state && !t.stateChangedAt().isAfter(cutoff))
                    .sorted(Comparator.comparing(TaskRecord::stateChangedAt).thenComparingLong(TaskRecord::id))
                    .limit(limit)
                    .toList();
            old.forEach(t -> removeKeepingKey(t, t.keyUntil(), now));
            return old.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int releaseKeys(int limit) {
        StoreArguments.requireLimit(limit);
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
            List<String> passed = keptKeys.entrySet().stream()
                    .filter(e -> !e.getValue().until().isAfter(now))
                    .sorted(Comparator.comparing((Map.Entry<String, KeptKey> e) ->
                                    e.getValue().until())
                            .thenComparingLong(e -> e.getValue().id()))
                    .limit(limit - expired.size())
                    .map(Map.Entry::getKey)
                    .toList();
            passed.forEach(keptKeys::remove);
            return expired.size() + passed.size();
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
                .map(TaskRecord::dueAt)
                .min(Comparator.naturalOrder())
                .orElse(null);
    }

    @Override
    public PutScheduleResult putSchedule(StoredSchedule definition, Instant nextFire, Instant keepUntil) {
        lock.lock();
        try {
            Recurring old = recurring.get(definition.scheduleId());
            boolean sameTiming = old != null
                    && Objects.equals(old.definition().cron(), definition.cron())
                    && Objects.equals(old.definition().zone(), definition.zone())
                    && Objects.equals(old.definition().fixedDelay(), definition.fixedDelay());
            TaskRecord running = sameTiming && old.occurrence() != null ? tasks.get(old.occurrence()) : null;
            boolean occurrenceRunning = running != null && !running.state().isTerminal();
            boolean keep = sameTiming && (occurrenceRunning || !old.nextFireAt().isAfter(millis(keepUntil)));
            Instant fire = keep ? old.nextFireAt() : nextFire;
            Long occurrence = occurrenceRunning ? old.occurrence() : null;
            if (keep
                    && old.definition()
                            .equals(definition
                                    .withPaused(old.definition().paused())
                                    .withRevision(old.definition().revision()))
                    && Objects.equals(occurrence, old.occurrence())) {
                return new PutScheduleResult(PutScheduleOutcome.UNCHANGED, fire);
            }
            StoredSchedule stored = definition
                    .withPaused(old != null ? old.definition().paused() : definition.paused())
                    .withRevision(++recurringRevision);
            Recurring saved = new Recurring(stored, fire, occurrence, keep && old.triggered());
            recurring.put(definition.scheduleId(), saved);
            return new PutScheduleResult(
                    old == null ? PutScheduleOutcome.CREATED : PutScheduleOutcome.CHANGED, saved.nextFireAt());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean deleteSchedule(String scheduleId) {
        lock.lock();
        try {
            return recurring.remove(scheduleId) != null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<String, Instant> nextFireTimes() {
        lock.lock();
        try {
            Map<String, Instant> result = new TreeMap<>();
            recurring.forEach((id, r) -> result.put(id, r.nextFireAt()));
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<DueSchedule> dueSchedules(Instant upTo, int limit) {
        StoreArguments.requireLimit(limit);
        lock.lock();
        try {
            return recurring.entrySet().stream()
                    .filter(e -> !e.getValue().definition().paused())
                    .filter(e -> !e.getValue().nextFireAt().isAfter(upTo))
                    .sorted(Comparator.comparing((Map.Entry<String, Recurring> e) ->
                                    e.getValue().nextFireAt())
                            .thenComparing(Map.Entry::getKey))
                    .limit(limit)
                    .map(e -> new DueSchedule(
                            e.getKey(),
                            e.getValue().nextFireAt(),
                            e.getValue().definition(),
                            e.getValue().triggered()))
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean commitSchedule(DueSchedule due, Instant nextFire, TaskRecord given) {
        StoreArguments.requireKey(given.uniqueKey());
        TaskRecord occurrence = given.scheduleId() != null
                ? given
                : given.toBuilder().scheduleId(due.scheduleId()).build();
        lock.lock();
        try {
            Recurring current = recurring.get(due.scheduleId());
            if (current == null
                    || current.definition().paused()
                    || current.definition().revision() != due.definition().revision()
                    || !current.nextFireAt().equals(millis(due.fireAt()))) {
                return false;
            }
            EnqueueOutcome outcome = enqueueLocked(occurrence, due.fireAt());
            if (outcome == EnqueueOutcome.EXISTS) {
                return false;
            }
            Long occurrenceId = current.occurrence();
            if (current.definition().isFixedDelay()) {
                if (outcome instanceof EnqueueOutcome.Stored) {
                    occurrenceId = occurrence.id();
                } else if (outcome instanceof EnqueueOutcome.DuplicateKey held && occurrenceId == null) {
                    TaskRecord holder = tasks.get(held.existingId());
                    if (holder != null
                            && !holder.state().isTerminal()
                            && due.scheduleId().equals(holder.scheduleId())) {
                        occurrenceId = held.existingId();
                    }
                }
            }
            recurring.put(due.scheduleId(), new Recurring(current.definition(), nextFire, occurrenceId));
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<TaskRecord> task(long id) {
        lock.lock();
        try {
            return Optional.ofNullable(tasks.get(id));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<TaskRecord> taskByKey(String key) {
        lock.lock();
        try {
            Long holderId = keyHolders.get(key);
            TaskRecord holder = holderId == null ? null : tasks.get(holderId);
            return holder != null && keyHeld(holder, clock.instant()) ? Optional.of(holder) : Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean holdsKey(long id, String key) {
        lock.lock();
        try {
            Instant now = clock.instant();
            Long holderId = keyHolders.get(key);
            TaskRecord holder = holderId == null ? null : tasks.get(holderId);
            if (holder != null && keyHeld(holder, now)) return holderId == id;
            Long keptBy = keptKeyHolder(key, now);
            return keptBy != null && keptBy == id;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<Integer, StateCounts> counts() {
        lock.lock();
        try {
            Map<Integer, long[]> raw = new TreeMap<>();
            for (int queueId : queueIds.values()) {
                raw.put(queueId, new long[TaskState.values().length]);
            }
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

    /**
     * The cursor is {@code <epoch second>.<nanosecond>:<id>} of the sort time and id of the last task of the
     * page; the sort time is due for QUEUED and SCHEDULED, stateChanged otherwise.
     */
    @Override
    public TaskPage tasks(TaskState state, Integer queueId, String cursor, int limit) {
        StoreArguments.requireLimit(limit);
        var position = cursor == null ? null : decodeCursor(cursor);
        Function<TaskRecord, Instant> sortTime = state == TaskState.QUEUED || state == TaskState.SCHEDULED
                ? TaskRecord::dueAt
                : TaskRecord::stateChangedAt;
        lock.lock();
        try {
            Comparator<TaskRecord> order = Comparator.comparing(sortTime).thenComparingLong(TaskRecord::id);
            List<TaskRecord> found = tasks.values().stream()
                    .filter(t -> t.state() == state)
                    .filter(t -> queueId == null || t.queueId() == queueId)
                    .filter(t -> position == null || isAfter(sortTime.apply(t), t.id(), position))
                    .sorted(order)
                    .limit((long) limit + 1)
                    .toList();
            if (found.size() <= limit) return new TaskPage(found, null);
            var items = found.subList(0, limit);
            var last = items.getLast();
            return new TaskPage(items, encodeCursor(sortTime.apply(last), last.id()));
        } finally {
            lock.unlock();
        }
    }

    private static String encodeCursor(Instant at, long id) {
        return at.getEpochSecond() + "." + at.getNano() + ":" + id;
    }

    private record Position(Instant at, long id) {}

    private record KeptKey(long id, Instant until) {
        KeptKey {
            until = millis(until);
        }
    }

    private static boolean isAfter(Instant at, long id, Position p) {
        int c = at.compareTo(p.at());
        return c > 0 || (c == 0 && id > p.id());
    }

    private static Position decodeCursor(String cursor) {
        var parts = cursor.split(":", -1);
        var time = parts[0].split("\\.", -1);
        try {
            if (parts.length == 2 && time.length == 2) {
                var nanos = Long.parseLong(time[1]);
                if (nanos >= 0 && nanos < 1_000_000_000L) {
                    return new Position(
                            Instant.ofEpochSecond(Long.parseLong(time[0]), nanos), Long.parseLong(parts[1]));
                }
            }
        } catch (NumberFormatException | DateTimeException e) {
            throw new TaskValidationException("malformed cursor");
        }
        throw new TaskValidationException("malformed cursor");
    }

    @Override
    public boolean finishCancelled(long id, int revision, String reason, boolean keepPayload, ScheduleEffect effect) {
        return finishCancelled(id, revision, reason, keepPayload, effect, false);
    }

    @Override
    public boolean finishCancelledExpired(
            long id, int revision, String reason, boolean keepPayload, ScheduleEffect effect) {
        return finishCancelled(id, revision, reason, keepPayload, effect, true);
    }

    private boolean finishCancelled(
            long id, int revision, String reason, boolean keepPayload, ScheduleEffect effect, boolean expiredOnly) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null
                    || !fenced(t, revision)
                    || (expiredOnly && t.leaseUntil().isAfter(clock.instant()))) {
                return false;
            }
            Instant now = clock.instant();
            applyEffect(effect);
            TaskRecord.Builder b = t.toBuilder()
                    .state(TaskState.CANCELLED)
                    .revision(t.revision() + 1)
                    .stateChangedAt(now)
                    .cancelReason(reason)
                    .cancelRequestedAt(t.cancelRequestedAt() != null ? t.cancelRequestedAt() : now)
                    .leaseUntil(null);
            if (!keepPayload) {
                b.encodedPayload(null);
            }
            finish(t, b, now);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<ExpiredTask> expire(int limit) {
        StoreArguments.requireLimit(limit);
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
                                .stateChangedAt(now),
                        now);
            }
            return overdue.stream()
                    .map(t -> new ExpiredTask(t.id(), t.queueId(), t.typeId()))
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int purgeRunners(Duration staleAfter) {
        lock.lock();
        try {
            Instant cutoff = clock.instant().minus(millis(staleAfter));
            int before = runners.size();
            runners.values().removeIf(w -> w.lastSeenAt().isBefore(cutoff));
            return before - runners.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CancelOutcome cancel(long id, int revision, String reason, ScheduleEffect effect) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null) {
                return CancelOutcome.NOT_FOUND;
            }
            if (t.revision() != revision) {
                return CancelOutcome.STALE;
            }
            Instant now = clock.instant();
            if (t.state() == TaskState.QUEUED || t.state() == TaskState.SCHEDULED) {
                finish(
                        t,
                        t.toBuilder()
                                .state(TaskState.CANCELLED)
                                .revision(t.revision() + 1)
                                .stateChangedAt(now)
                                .cancelReason(reason)
                                .cancelRequestedAt(now),
                        now);
                applyEffect(effect);
                return CancelOutcome.CANCELLED;
            }
            if (t.state() == TaskState.LEASED) {
                save(t.toBuilder().cancelReason(reason).cancelRequestedAt(now).build());
                return CancelOutcome.REQUESTED;
            }
            return CancelOutcome.FINISHED;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RetryOutcome retry(long id, int revision) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null) {
                return RetryOutcome.NOT_FOUND;
            }
            if (t.revision() != revision) {
                return RetryOutcome.STALE;
            }
            if (t.state() != TaskState.DEAD && t.state() != TaskState.CANCELLED && t.state() != TaskState.EXPIRED) {
                return RetryOutcome.NOT_RETRYABLE;
            }
            Instant now = clock.instant();
            String key = t.uniqueKey();
            if (key != null && t.keyUntil() != null && !t.keyUntil().isAfter(now)) {
                releaseKey(id);
                t = tasks.get(id);
                key = null;
            }
            if (key != null) {
                Long holderId = keyHolders.get(key);
                if (holderId != null && holderId != id) {
                    TaskRecord holder = tasks.get(holderId);
                    if (holder != null && keyHeld(holder, now)) {
                        throw keyHeldByAnother(id, holderId);
                    }
                    releaseKey(holderId);
                }
                Long keptBy = keptKeyHolder(key, now);
                if (keptBy != null) {
                    throw keyHeldByAnother(id, keptBy);
                }
                keyHolders.put(key, id);
            }
            save(t.toBuilder()
                    .state(TaskState.QUEUED)
                    .dueAt(now)
                    .stateChangedAt(now)
                    .attempts(0)
                    .revision(t.revision() + 1)
                    .cancelRequestedAt(null)
                    .cancelReason(null)
                    .runnerId(null)
                    .leaseUntil(null)
                    .keyKeep(key != null ? t.keyKeep() : null)
                    .keyUntil(null)
                    .startBy(null)
                    .build());
            return RetryOutcome.RETRIED;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public DeleteOutcome delete(long id, int revision, ScheduleEffect effect) {
        lock.lock();
        try {
            TaskRecord t = tasks.get(id);
            if (t == null) {
                return DeleteOutcome.NOT_FOUND;
            }
            if (t.revision() != revision) {
                return DeleteOutcome.STALE;
            }
            if (t.state() == TaskState.LEASED) {
                return DeleteOutcome.LEASED;
            }
            remove(t);
            applyEffect(effect);
            return DeleteOutcome.DELETED;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void pauseQueue(int queueId, boolean paused) {
        lock.lock();
        try {
            if (paused) {
                if (queueIds.containsValue(queueId)) pausedQueues.add(queueId);
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
    public void heartbeat(
            int runnerId, String label, Set<Integer> queueIds, Set<Integer> typeIds, int running, boolean accepting) {
        StoreArguments.requireLabel(label);
        lock.lock();
        try {
            Instant now = clock.instant();
            RunnerRecord w = runners.get(runnerId);
            runners.put(
                    runnerId,
                    w == null
                            ? new RunnerRecord(runnerId, label, now, now, running, accepting, queueIds, typeIds)
                            : new RunnerRecord(
                                    w.id(),
                                    w.label(),
                                    w.startedAt(),
                                    now,
                                    running,
                                    accepting,
                                    w.queueIds(),
                                    w.typeIds()));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<Integer, Map<Integer, Long>> queuedByType() {
        lock.lock();
        try {
            var result = new TreeMap<Integer, Map<Integer, Long>>();
            for (var task : tasks.values()) {
                if (task.state() == TaskState.QUEUED) {
                    result.computeIfAbsent(task.queueId(), q -> new TreeMap<>()).merge(task.typeId(), 1L, Long::sum);
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<RunnerRecord> runners() {
        lock.lock();
        try {
            return List.copyOf(runners.values());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<ScheduleEntry> schedules() {
        lock.lock();
        try {
            return recurring.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey(ScheduleStore.ID_ORDER))
                    .map(e -> new ScheduleEntry(
                            e.getValue().definition(), e.getValue().nextFireAt()))
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean pauseSchedule(String scheduleId) {
        lock.lock();
        try {
            Recurring r = recurring.get(scheduleId);
            if (r == null) {
                return false;
            }
            recurring.put(scheduleId, r.withDefinition(r.definition().withPaused(true)));
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean resumeSchedule(String scheduleId, Instant nextFire) {
        lock.lock();
        try {
            Recurring r = recurring.get(scheduleId);
            if (r == null) {
                return false;
            }
            if (!r.definition().paused()) {
                return true;
            }
            StoredSchedule resumed = r.definition().withPaused(false);
            TaskRecord occurrence = r.occurrence() == null ? null : tasks.get(r.occurrence());
            boolean occurrenceRunning =
                    occurrence != null && !occurrence.state().isTerminal();
            recurring.put(
                    scheduleId,
                    nextFire == null || occurrenceRunning
                            ? r.withDefinition(resumed)
                            : new Recurring(resumed.withRevision(++recurringRevision), nextFire, r.occurrence()));
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean triggerSchedule(String scheduleId, Instant fireAt) {
        lock.lock();
        try {
            Recurring r = recurring.get(scheduleId);
            if (r == null) {
                return false;
            }
            recurring.put(
                    scheduleId, new Recurring(r.definition().withRevision(++recurringRevision), fireAt, null, true));
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean setNextFire(String scheduleId, long revision, Instant expectedFire, Instant nextFire) {
        lock.lock();
        try {
            Recurring r = recurring.get(scheduleId);
            if (r == null
                    || r.definition().revision() != revision
                    || !r.nextFireAt().equals(millis(expectedFire))
                    || r.definition().paused()) {
                return false;
            }
            recurring.put(scheduleId, r.withNext(nextFire));
            return true;
        } finally {
            lock.unlock();
        }
    }
}
