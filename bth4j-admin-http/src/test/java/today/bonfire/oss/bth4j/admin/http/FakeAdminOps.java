package today.bonfire.oss.bth4j.admin.http;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.Issue;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.ScheduleView;
import today.bonfire.oss.bth4j.TaskView;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

final class FakeAdminOps implements AdminOps {

    final TreeMap<Long, TaskView> tasks = new TreeMap<>();
    final Map<String, StateCounts> counts = new ConcurrentHashMap<>();
    final List<QueueInfo> queues = new CopyOnWriteArrayList<>();
    final List<ScheduleView> schedules = new CopyOnWriteArrayList<>();
    final List<WorkerInfo> workers = new CopyOnWriteArrayList<>();
    final List<Issue> issues = new CopyOnWriteArrayList<>();
    final List<String> calls = new CopyOnWriteArrayList<>();
    volatile RunnerHealth health;
    volatile RuntimeException failure;

    static TaskView task(long id, int revision, TaskState state, String queue, String data) {
        var record = TaskRecord.builder()
                .id(id)
                .revision(revision)
                .state(state)
                .attempts(1)
                .maxAttempts(3)
                .created(Instant.parse("2026-01-01T00:00:00Z"))
                .due(Instant.parse("2026-01-01T00:00:01Z"))
                .stateChanged(Instant.parse("2026-01-01T00:00:02Z"))
                .data(data)
                .maxRun(Duration.ofMinutes(5))
                .build();
        return new TaskView(record, queue, "send-mail");
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
        calls.add("pauseQueue " + queue + ' ' + paused);
    }

    @Override
    public Optional<TaskView> task(long id) {
        if (failure != null) {
            throw failure;
        }
        return Optional.ofNullable(tasks.get(id));
    }

    @Override
    public List<TaskView> tasks(TaskState state, String queue, long afterId, int limit) {
        calls.add("tasks " + state + ' ' + queue + ' ' + afterId + ' ' + limit);
        var out = new ArrayList<TaskView>();
        for (var t : tasks.tailMap(afterId, false).values()) {
            if (t.state() == state && (queue == null || queue.equals(t.queue())) && out.size() < limit) {
                out.add(t);
            }
        }
        return out;
    }

    @Override
    public boolean retry(long id, int revision) {
        calls.add("retry " + id + ' ' + revision);
        var t = tasks.get(id);
        return t != null && t.revision() == revision;
    }

    @Override
    public CancelOutcome cancel(long id, int revision, String reason) {
        calls.add("cancel " + id + ' ' + revision + ' ' + reason);
        var t = tasks.get(id);
        return t != null && t.revision() == revision ? CancelOutcome.REQUESTED : CancelOutcome.STALE;
    }

    @Override
    public boolean delete(long id, int revision) {
        calls.add("delete " + id + ' ' + revision);
        var t = tasks.get(id);
        return t != null && t.revision() == revision;
    }

    @Override
    public List<WorkerInfo> workers() {
        return workers;
    }

    @Override
    public int purgeStaleWorkers(Duration staleAfter) {
        return 0;
    }

    @Override
    public List<ScheduleView> schedules() {
        return schedules;
    }

    @Override
    public boolean pauseSchedule(String id, boolean paused) {
        calls.add("pauseSchedule " + id + ' ' + paused);
        return schedules.stream().anyMatch(s -> s.id().equals(id));
    }

    @Override
    public boolean triggerSchedule(String id) {
        calls.add("triggerSchedule " + id);
        return schedules.stream().anyMatch(s -> s.id().equals(id));
    }

    @Override
    public boolean deleteSchedule(String id) {
        calls.add("deleteSchedule " + id);
        return schedules.stream().anyMatch(s -> s.id().equals(id));
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
        return Map.of();
    }

    @Override
    public Map<Integer, String> eventNames() {
        return Map.of();
    }
}
