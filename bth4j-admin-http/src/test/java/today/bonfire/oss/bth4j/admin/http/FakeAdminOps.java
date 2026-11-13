package today.bonfire.oss.bth4j.admin.http;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.CancelOutcome;
import today.bonfire.oss.bth4j.DeleteOutcome;
import today.bonfire.oss.bth4j.Issue;
import today.bonfire.oss.bth4j.QueueInfo;
import today.bonfire.oss.bth4j.RetryOutcome;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.RunnerView;
import today.bonfire.oss.bth4j.ScheduleOutcome;
import today.bonfire.oss.bth4j.ScheduleView;
import today.bonfire.oss.bth4j.ScheduleViewPage;
import today.bonfire.oss.bth4j.StateCounts;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.TaskView;
import today.bonfire.oss.bth4j.TaskViewPage;
import today.bonfire.oss.bth4j.UnservedWork;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.ScheduleStore;

final class FakeAdminOps implements AdminOps {

    final TreeMap<Long, TaskView> tasks = new TreeMap<>();
    final Map<String, Long> keyHolders = new ConcurrentHashMap<>();
    final java.util.Set<String> staleKeys = ConcurrentHashMap.newKeySet();
    final Map<String, StateCounts> counts = new ConcurrentHashMap<>();
    final List<QueueInfo> queues = new CopyOnWriteArrayList<>();
    final List<ScheduleView> schedules = new CopyOnWriteArrayList<>();
    final List<UnservedWork> unserved = new CopyOnWriteArrayList<>();
    final List<RunnerView> runners = new CopyOnWriteArrayList<>();
    final List<Issue> issues = new CopyOnWriteArrayList<>();
    final List<String> calls = new CopyOnWriteArrayList<>();
    volatile int taskLookups;
    volatile RunnerHealth health;
    volatile RuntimeException failure;
    volatile RuntimeException queueFailure;

    static TaskView task(long id, int revision, TaskState state, String queue, String encodedPayload) {
        return TaskView.builder()
                .id(id)
                .revision(revision)
                .state(state)
                .attempts(1)
                .maxAttempts(3)
                .createdAt(Instant.parse("2026-01-01T00:00:00Z"))
                .dueAt(Instant.parse("2026-01-01T00:00:01Z"))
                .stateChangedAt(Instant.parse("2026-01-01T00:00:02Z"))
                .encodedPayload(encodedPayload)
                .maxRunTime(Duration.ofMinutes(5))
                .queue(queue)
                .typeName("send-mail")
                .build();
    }

    @Override
    public Map<String, StateCounts> counts() {
        return counts;
    }

    @Override
    public List<QueueInfo> queues() {
        return queues;
    }

    @Override
    public void pauseQueue(String queue, boolean paused) {
        if (queueFailure != null) {
            throw queueFailure;
        }
        calls.add("pauseQueue " + queue + ' ' + paused);
    }

    @Override
    public Optional<TaskView> task(long id) {
        taskLookups++;
        if (failure != null) {
            throw failure;
        }
        return Optional.ofNullable(tasks.get(id));
    }

    @Override
    public Optional<TaskView> taskByKey(String key) {
        if (failure != null) {
            throw failure;
        }
        return Optional.ofNullable(keyHolders.get(key)).map(tasks::get);
    }

    @Override
    public CancelOutcome cancelByKey(String key, String reason) {
        calls.add("cancelByKey " + key + ' ' + reason);
        var holder = taskByKey(key);
        if (holder.isEmpty()) return CancelOutcome.NOT_FOUND;
        if (holder.get().state().isTerminal()) return CancelOutcome.FINISHED;
        return staleKeys.contains(key) ? CancelOutcome.STALE : CancelOutcome.REQUESTED;
    }

    @Override
    public TaskViewPage tasks(TaskState state, String cursor, int limit) {
        return list(state, null, cursor, limit);
    }

    @Override
    public TaskViewPage tasks(TaskState state, String queue, String cursor, int limit) {
        if (queue == null || queue.isBlank()) throw new TaskValidationException("queue is required");
        return list(state, queue, cursor, limit);
    }

    private TaskViewPage list(TaskState state, String queue, String cursor, int limit) {
        if (queueFailure != null) {
            throw queueFailure;
        }
        calls.add("tasks " + state + ' ' + queue + " cursor=" + cursor + ' ' + limit);
        long afterId;
        try {
            afterId = cursor == null ? 0 : Long.parseLong(cursor);
        } catch (NumberFormatException e) {
            throw new TaskValidationException("malformed cursor");
        }
        var out = new ArrayList<TaskView>();
        for (var t : tasks.tailMap(afterId, false).values()) {
            if (t.state() == state && (queue == null || queue.equals(t.queue())) && out.size() < limit) {
                out.add(t);
            }
        }
        return new TaskViewPage(
                out, out.size() < limit ? null : String.valueOf(out.getLast().id()));
    }

    @Override
    public RetryOutcome retry(long id, int revision) {
        calls.add("retry " + id + ' ' + revision);
        var t = tasks.get(id);
        if (t == null) return RetryOutcome.NOT_FOUND;
        if (t.revision() != revision) return RetryOutcome.STALE;
        if (t.state() != TaskState.DEAD && t.state() != TaskState.CANCELLED && t.state() != TaskState.EXPIRED) {
            return RetryOutcome.NOT_RETRYABLE;
        }
        return RetryOutcome.RETRIED;
    }

    @Override
    public CancelOutcome cancel(long id, int revision, String reason) {
        calls.add("cancel " + id + ' ' + revision + ' ' + reason);
        var t = tasks.get(id);
        if (t == null) return CancelOutcome.NOT_FOUND;
        if (t.revision() != revision) return CancelOutcome.STALE;
        return t.state().isTerminal() ? CancelOutcome.FINISHED : CancelOutcome.REQUESTED;
    }

    @Override
    public DeleteOutcome delete(long id, int revision) {
        calls.add("delete " + id + ' ' + revision);
        var t = tasks.get(id);
        if (t == null) return DeleteOutcome.NOT_FOUND;
        if (t.revision() != revision) return DeleteOutcome.STALE;
        return t.state() == TaskState.LEASED ? DeleteOutcome.LEASED : DeleteOutcome.DELETED;
    }

    @Override
    public List<RunnerView> runners() {
        return runners;
    }

    @Override
    public List<UnservedWork> unservedWork(Duration liveWithin) {
        calls.add("unservedWork " + liveWithin);
        return unserved;
    }

    @Override
    public int purgeStaleRunners(Duration staleAfter) {
        return 0;
    }

    @Override
    public ScheduleViewPage schedules(String cursor, int limit) {
        calls.add("schedules cursor=" + cursor + ' ' + limit);
        var behind = schedules.stream()
                .filter(s -> cursor == null || ScheduleStore.ID_ORDER.compare(s.id(), cursor) > 0)
                .sorted(Comparator.comparing(ScheduleView::id, ScheduleStore.ID_ORDER))
                .toList();
        var page = behind.stream().limit(limit).toList();
        return new ScheduleViewPage(page, behind.size() > limit ? page.getLast().id() : null);
    }

    @Override
    public List<ScheduleView> upcomingSchedules(int limit) {
        calls.add("upcomingSchedules " + limit);
        return schedules.stream()
                .filter(s -> !s.paused())
                .sorted(Comparator.comparing(ScheduleView::nextFireAt)
                        .thenComparing(ScheduleView::id, ScheduleStore.ID_ORDER))
                .limit(limit)
                .toList();
    }

    @Override
    public ScheduleOutcome pauseSchedule(String id, long revision, boolean paused) {
        calls.add("pauseSchedule " + id + ' ' + revision + ' ' + paused);
        return scheduleOutcome(id, revision);
    }

    @Override
    public ScheduleOutcome triggerSchedule(String id, long revision) {
        calls.add("triggerSchedule " + id + ' ' + revision);
        return scheduleOutcome(id, revision);
    }

    @Override
    public ScheduleOutcome deleteSchedule(String id, long revision) {
        calls.add("deleteSchedule " + id + ' ' + revision);
        return scheduleOutcome(id, revision);
    }

    private ScheduleOutcome scheduleOutcome(String id, long revision) {
        var found = schedules.stream().filter(s -> s.id().equals(id)).findFirst();
        if (found.isEmpty()) return ScheduleOutcome.NOT_FOUND;
        return found.get().revision() == revision ? ScheduleOutcome.APPLIED : ScheduleOutcome.STALE;
    }

    @Override
    public Optional<RunnerHealth> health() {
        return Optional.ofNullable(health);
    }

    @Override
    public List<Issue> issues() {
        return issues;
    }

    @Override
    public Map<Integer, String> queueNames() {
        return Map.of(1, "default", 2, "mail");
    }

    @Override
    public Map<Integer, String> typeNames() {
        return Map.of(1, "send-mail", 2, "export");
    }
}
