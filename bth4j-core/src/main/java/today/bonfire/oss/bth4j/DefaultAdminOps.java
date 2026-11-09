package today.bonfire.oss.bth4j;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.RecurringSchedule;
import today.bonfire.oss.bth4j.spi.RetryOutcome;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/** {@link AdminOps} over a store; health and issues come from the runner when there is one. */
final class DefaultAdminOps implements AdminOps {
    private static final int MAX_REASON = 512;

    private final FullStore store;
    private final Names names;
    private final QueuesHolder queues;
    private final Clock clock;
    private final Supplier<RunnerHealth> health;
    private final Supplier<List<Issue>> issues;

    DefaultAdminOps(
            FullStore store,
            Names names,
            QueuesHolder queues,
            Clock clock,
            Supplier<RunnerHealth> health,
            Supplier<List<Issue>> issues) {
        this.store = store;
        this.names = names;
        this.queues = queues;
        this.clock = clock;
        this.health = health;
        this.issues = issues;
    }

    @Override
    public Map<String, StateCounts> counts() {
        var result = new LinkedHashMap<String, StateCounts>();
        store.counts().forEach((id, c) -> result.put(names.queueName(id), c));
        return result;
    }

    @Override
    public List<QueueInfo> queues() {
        return store.queues();
    }

    @Override
    public void pauseQueue(String queue, boolean paused) {
        store.pauseQueue(adminQueueId(queue), paused);
    }

    /** A blank name is the default queue; any other name must already be in the store's registry. */
    private int adminQueueId(String queue) {
        var name = queue == null || queue.isBlank() ? queues.defaultQueue : queue;
        return names.knownQueueId(name).orElseThrow(() -> new TaskValidationException("Unknown queue: " + name));
    }

    @Override
    public Optional<TaskView> task(long id) {
        return store.getTask(id).map(names::view);
    }

    @Override
    public Optional<TaskView> taskByKey(String key) {
        StoredText.require("uniqueKey", key);
        return store.taskByKey(key).map(names::view);
    }

    @Override
    public TaskViewPage tasks(TaskState state, String queue, String cursor, int limit) {
        var queueId = queue == null ? null : Integer.valueOf(adminQueueId(queue));
        var page = store.tasks(state, queueId, cursor, limit);
        var result = new ArrayList<TaskView>();
        for (var r : page.items()) result.add(names.view(r));
        return new TaskViewPage(result, page.next());
    }

    @Override
    public RetryOutcome retry(long id, int revision) {
        return store.retry(id, revision);
    }

    @Override
    public CancelOutcome cancel(long id, int revision, String reason) {
        if (reason != null && reason.length() > MAX_REASON) {
            throw new TaskValidationException("reason longer than " + MAX_REASON + " characters");
        }
        StoredText.require("reason", reason);
        // a waiting fixed-delay occurrence re-arms nothing, so the store pauses its definition with the cancel
        var pause = RecurringTaskService.pauseEffect(readable(id));
        return store.cancel(id, revision, reason == null || reason.isBlank() ? "cancelled by operator" : reason, pause);
    }

    @Override
    public CancelOutcome cancelByKey(String key, String reason) {
        StoredText.require("uniqueKey", key);
        var holder = store.taskByKey(key).orElse(null);
        if (holder == null) return CancelOutcome.NOT_FOUND;
        if (holder.state().isTerminal()) return CancelOutcome.FINISHED;
        return cancel(holder.id(), holder.revision(), reason);
    }

    @Override
    public boolean delete(long id, int revision) {
        var pause = RecurringTaskService.pauseEffect(readable(id));
        return store.delete(id, revision, pause);
    }

    /**
     * The task as far as a schedule effect needs it; null when it is missing or cannot be read, so that an
     * operator can still cancel or delete a task with a corrupt stored value. Such a task is not treated as a
     * fixed-delay occurrence.
     */
    private TaskRecord readable(long id) {
        try {
            return store.getTask(id).orElse(null);
        } catch (UnreadableTaskException e) {
            return null;
        }
    }

    @Override
    public List<WorkerInfo> workers() {
        return store.workers();
    }

    @Override
    public List<UnservedWork> unservedWork(Duration liveWithin) {
        if (liveWithin.isNegative()) throw new IllegalArgumentException("liveWithin must be non-negative");
        var cutoff = clock.instant().minus(liveWithin);
        var live = store.workers().stream()
                .filter(w -> w.accepting() && !w.lastSeen().isBefore(cutoff))
                .toList();
        var paused = new HashSet<Integer>();
        for (var queue : store.queues()) {
            if (queue.paused()) paused.add(queue.id());
        }
        var result = new ArrayList<UnservedWork>();
        for (var entry : store.queuedByType().entrySet()) {
            int queueId = entry.getKey();
            long waiting = entry.getValue().values().stream()
                    .mapToLong(Long::longValue)
                    .sum();
            if (waiting == 0 || paused.contains(queueId)) continue;
            var workers =
                    live.stream().filter(w -> w.queueIds().contains(queueId)).toList();
            if (workers.isEmpty()) {
                result.add(new UnservedWork(names.queueName(queueId), null, waiting));
                continue;
            }
            var types = new HashSet<Integer>();
            for (var worker : workers) types.addAll(worker.typeIds());
            entry.getValue().forEach((type, count) -> {
                if (!types.contains(type)) {
                    result.add(new UnservedWork(names.queueName(queueId), names.typeName(type), count));
                }
            });
        }
        return result;
    }

    @Override
    public int purgeStaleWorkers(Duration staleAfter) {
        if (staleAfter == null || staleAfter.isNegative() || staleAfter.isZero() || !StoredTime.fits(staleAfter)) {
            throw new IllegalArgumentException("staleAfter must be positive and at most " + StoredTime.MAX);
        }
        return store.purgeWorkers(staleAfter);
    }

    @Override
    public List<ScheduleView> schedules() {
        var result = new ArrayList<ScheduleView>();
        for (var s : store.recurringDefinitions()) result.add(view(s));
        return result;
    }

    private ScheduleView view(RecurringSchedule s) {
        var d = s.definition();
        return new ScheduleView(
                d.scheduleId(),
                names.typeName(d.typeId()),
                names.queueName(d.queueId()),
                d.account(),
                d.cron(),
                d.zone() == null ? null : ZoneId.of(d.zone()),
                RecurringDefinition.misfire(d.misfire(), d.catchUpMaxFires()),
                d.fixedDelayMs() == null ? null : Duration.ofMillis(d.fixedDelayMs()),
                d.overlapSkip() ? Overlap.SKIP : Overlap.ALLOW,
                d.paused(),
                s.nextFire(),
                d.maxAttempts(),
                d.maxRunTimeMs() == null ? null : Duration.ofMillis(d.maxRunTimeMs()));
    }

    @Override
    public boolean pauseSchedule(String id, boolean paused) {
        if (paused) return store.pauseRecurring(id);
        // a fixed-delay definition parks its next fire while paused, so it resumes one delay from now; the store
        // applies that only to a definition that is paused, so resuming a running one changes nothing
        var next = store.recurringDefinitions().stream()
                .filter(s ->
                        s.definition().scheduleId().equals(id) && s.definition().isFixedDelay())
                .findFirst()
                .map(s -> clock.instant().plusMillis(s.definition().fixedDelayMs()))
                .orElse(null);
        return store.resumeRecurring(id, next);
    }

    @Override
    public boolean triggerSchedule(String id) {
        return store.setNextFire(id, clock.instant());
    }

    @Override
    public boolean deleteSchedule(String id) {
        return store.deleteRecurring(id);
    }

    @Override
    public Optional<RunnerHealth> health() {
        return Optional.ofNullable(health == null ? null : health.get());
    }

    @Override
    public List<Issue> issues() {
        return issues == null ? List.of() : issues.get();
    }

    @Override
    public Map<Integer, String> queueNames() {
        return store.queueNames();
    }

    @Override
    public Map<Integer, String> typeNames() {
        return store.typeNames();
    }
}
