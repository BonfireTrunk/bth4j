package today.bonfire.oss.bth4j.service;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.Issue;
import today.bonfire.oss.bth4j.Overlap;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.ScheduleView;
import today.bonfire.oss.bth4j.TaskView;
import today.bonfire.oss.bth4j.TaskViewPage;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.RecurringSchedule;
import today.bonfire.oss.bth4j.spi.RetryOutcome;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/** {@link AdminOps} over a store; health and issues come from the runner when there is one. */
public final class DefaultAdminOps implements AdminOps {
    private static final int MAX_REASON = 512;

    private final FullStore store;
    private final Names names;
    private final QueuesHolder queues;
    private final Clock clock;
    private final Supplier<RunnerHealth> health;
    private final Supplier<List<Issue>> issues;

    public DefaultAdminOps(
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
        store.pauseQueue(names.queueId(queues.validQueue(queue)), paused);
    }

    @Override
    public Optional<TaskView> task(long id) {
        return store.getTask(id).map(names::view);
    }

    @Override
    public Optional<TaskView> taskByKey(String key) {
        return store.taskByKey(key).map(names::view);
    }

    @Override
    public TaskViewPage tasks(TaskState state, String queue, String cursor, int limit) {
        var queueId = queue == null ? null : Integer.valueOf(names.queueId(queues.validQueue(queue)));
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
        // a waiting fixed-delay occurrence re-arms nothing, so the store pauses its definition with the cancel
        var pause = RecurringTaskService.pauseEffect(store.getTask(id).orElse(null));
        return store.cancel(id, revision, reason == null || reason.isBlank() ? "cancelled by operator" : reason, pause);
    }

    @Override
    public CancelOutcome cancelByKey(String key, String reason) {
        var holder = store.taskByKey(key).orElse(null);
        if (holder == null) return CancelOutcome.NOT_FOUND;
        if (holder.state().isTerminal()) return CancelOutcome.FINISHED;
        return cancel(holder.id(), holder.revision(), reason);
    }

    @Override
    public boolean delete(long id, int revision) {
        var pause = RecurringTaskService.pauseEffect(store.getTask(id).orElse(null));
        return store.delete(id, revision, pause);
    }

    @Override
    public List<WorkerInfo> workers() {
        return store.workers();
    }

    @Override
    public int purgeStaleWorkers(Duration staleAfter) {
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
                Misfires.fromCode(d.misfire(), d.catchUpMaxFires()),
                d.fixedDelayMs() == null ? null : Duration.ofMillis(d.fixedDelayMs()),
                d.overlapSkip() ? Overlap.SKIP : Overlap.ALLOW,
                d.paused(),
                s.nextFire(),
                d.maxAttempts(),
                d.maxRunTimeMs() == null ? null : Duration.ofMillis(d.maxRunTimeMs()));
    }

    @Override
    public boolean pauseSchedule(String id, boolean paused) {
        if (!paused) {
            // a fixed-delay definition parks its next fire while paused; the re-arm goes first, while it is
            // still paused, so the runner cannot commit an occurrence in between and fire again one delay later
            store.recurringDefinitions().stream()
                    .filter(s -> s.definition().scheduleId().equals(id)
                            && s.definition().isFixedDelay())
                    .findFirst()
                    .ifPresent(s -> store.setNextFire(
                            id, clock.instant().plusMillis(s.definition().fixedDelayMs())));
        }
        return store.pauseRecurring(id, paused);
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
