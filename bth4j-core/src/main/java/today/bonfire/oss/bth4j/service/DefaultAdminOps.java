package today.bonfire.oss.bth4j.service;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.Issue;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.ScheduleView;
import today.bonfire.oss.bth4j.TaskView;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.RecurringSchedule;
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
        store.pauseQueue(names.queueId(queues.getValidQueue(queue)), paused);
    }

    @Override
    public Optional<TaskView> task(long id) {
        return store.getTask(id).map(names::view);
    }

    @Override
    public List<TaskView> tasks(TaskState state, String queue, long afterId, int limit) {
        var queueId = queue == null ? null : Integer.valueOf(names.queueId(queues.getValidQueue(queue)));
        var result = new ArrayList<TaskView>();
        for (var r : store.tasks(state, queueId, afterId, limit)) result.add(names.view(r));
        return result;
    }

    @Override
    public boolean retry(long id, int revision) {
        return store.retry(id, revision);
    }

    @Override
    public CancelOutcome cancel(long id, int revision, String reason) {
        if (reason != null && reason.length() > MAX_REASON) {
            throw new TaskValidationException("reason longer than " + MAX_REASON + " characters");
        }
        return store.cancel(id, revision, reason == null || reason.isBlank() ? "cancelled by operator" : reason);
    }

    @Override
    public boolean delete(long id, int revision) {
        return store.delete(id, revision);
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
                names.eventName(d.eventId()),
                names.queueName(d.queueId()),
                d.account(),
                d.cron(),
                d.zone(),
                d.misfire(),
                d.fixedDelayMs() == null ? null : Duration.ofMillis(d.fixedDelayMs()),
                d.overlapSkip(),
                d.paused(),
                s.nextFire(),
                d.maxAttempts());
    }

    @Override
    public boolean pauseSchedule(String id, boolean paused) {
        if (!store.pauseRecurring(id, paused)) return false;
        if (!paused) {
            // a fixed-delay definition parks its next fire while paused; resuming re-arms it from now
            store.recurringDefinitions().stream()
                    .filter(s -> s.definition().scheduleId().equals(id)
                            && s.definition().isFixedDelay())
                    .findFirst()
                    .ifPresent(s -> store.setNextFire(
                            id, clock.instant().plusMillis(s.definition().fixedDelayMs())));
        }
        return true;
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
    public Map<Integer, String> eventNames() {
        return store.eventNames();
    }
}
