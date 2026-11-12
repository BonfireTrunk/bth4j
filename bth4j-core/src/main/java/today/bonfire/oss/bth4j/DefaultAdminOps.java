package today.bonfire.oss.bth4j;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.ScheduleEntry;
import today.bonfire.oss.bth4j.spi.StoredSchedule;
import today.bonfire.oss.bth4j.spi.TaskPage;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/** {@link AdminOps} over a store; health and issues come from the runner when there is one. */
final class DefaultAdminOps implements AdminOps {
    private static final int MAX_REASON = 512;

    private final FullStore store;
    private final Names names;
    private final Clock clock;
    private final Supplier<RunnerHealth> health;
    private final Supplier<List<Issue>> issues;

    DefaultAdminOps(
            FullStore store, Names names, Clock clock, Supplier<RunnerHealth> health, Supplier<List<Issue>> issues) {
        this.store = store;
        this.names = names;
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

    /** The name must be given and already be in the store's registry. */
    private int adminQueueId(String queue) {
        if (queue == null || queue.isBlank()) throw new TaskValidationException("queue is required");
        return names.knownQueueId(queue).orElseThrow(() -> new TaskValidationException("Unknown queue: " + queue));
    }

    @Override
    public Optional<TaskView> task(long id) {
        return store.getTask(id).map(names::view);
    }

    @Override
    public Optional<TaskView> taskByKey(String key) {
        TaskClient.requireLookupKey(key);
        return store.taskByKey(key).map(names::view);
    }

    @Override
    public TaskViewPage tasks(TaskState state, String cursor, int limit) {
        return view(store.tasks(state, null, cursor, limit));
    }

    @Override
    public TaskViewPage tasks(TaskState state, String queue, String cursor, int limit) {
        return view(store.tasks(state, adminQueueId(queue), cursor, limit));
    }

    private TaskViewPage view(TaskPage page) {
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
        TaskClient.requireLookupKey(key);
        var holder = store.taskByKey(key).orElse(null);
        if (holder == null) return CancelOutcome.NOT_FOUND;
        if (holder.state().isTerminal()) return CancelOutcome.FINISHED;
        return cancel(holder.id(), holder.revision(), reason);
    }

    @Override
    public DeleteOutcome delete(long id, int revision) {
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
        if (liveWithin == null || liveWithin.isNegative() || !StoredTime.fits(liveWithin)) {
            throw new TaskValidationException("liveWithin must be non-negative and at most " + StoredTime.MAX);
        }
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
            throw new TaskValidationException("staleAfter must be positive and at most " + StoredTime.MAX);
        }
        return store.purgeWorkers(staleAfter);
    }

    @Override
    public List<ScheduleView> schedules() {
        var result = new ArrayList<ScheduleView>();
        for (var s : store.schedules()) result.add(view(s));
        return result;
    }

    private ScheduleView view(ScheduleEntry s) {
        var d = s.definition();
        return new ScheduleView(
                d.scheduleId(),
                names.typeName(d.typeId()),
                names.queueName(d.queueId()),
                d.account(),
                d.cron(),
                d.zone(),
                StoredSchedule.misfire(d.misfire(), d.catchUpMaxFires()),
                d.fixedDelay(),
                d.overlapSkip() ? Overlap.SKIP : Overlap.ALLOW,
                d.paused(),
                s.nextFireAt(),
                d.maxAttempts(),
                d.maxRunTime());
    }

    @Override
    public boolean pauseSchedule(String id, boolean paused) {
        StoredText.requirePresent("schedule id", id);
        if (paused) return store.pauseSchedule(id);
        // a fixed-delay definition parks its next fire while paused, so it resumes one delay from now; the store
        // applies that only to a paused definition whose occurrence is not still running
        var next = store.schedules().stream()
                .filter(s ->
                        s.definition().scheduleId().equals(id) && s.definition().isFixedDelay())
                .findFirst()
                .map(s -> clock.instant().plus(s.definition().fixedDelay()))
                .orElse(null);
        return store.resumeSchedule(id, next);
    }

    @Override
    public boolean triggerSchedule(String id) {
        StoredText.requirePresent("schedule id", id);
        return store.triggerSchedule(id, clock.instant());
    }

    @Override
    public boolean deleteSchedule(String id) {
        StoredText.requirePresent("schedule id", id);
        return store.deleteSchedule(id);
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
