package today.bonfire.oss.bth4j.admin.http;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import today.bonfire.oss.bth4j.Issue;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.ScheduleView;
import today.bonfire.oss.bth4j.TaskView;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/** Maps core types to the JSON structures of the API: ids and revisions as strings, times and durations as ISO-8601. */
final class Views {

    private Views() {}

    static Map<String, Object> obj(Object... keyValues) {
        var map = new LinkedHashMap<String, Object>();
        for (var i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private static String text(Object value) {
        return value == null ? null : value.toString();
    }

    private static String text(Duration value) {
        return value == null ? null : value.toString();
    }

    static Map<String, Object> task(TaskView t) {
        var steps = new LinkedHashMap<String, Object>();
        t.steps().forEach((name, step) -> steps.put(name, obj("at", step.at(), "result", step.result())));
        var failures = new ArrayList<Object>();
        for (var f : t.failures()) {
            failures.add(obj(
                    "attempt",
                    f.attempt(),
                    "at",
                    f.at(),
                    "type",
                    f.type(),
                    "message",
                    f.message(),
                    "stack",
                    f.stack()));
        }
        return obj(
                "id", text(t.id()),
                "revision", text(t.revision()),
                "state", t.state().name(),
                "queue", t.queue(),
                "event", t.event(),
                "payloadVersion", t.payloadVersion(),
                "attempts", t.attempts(),
                "maxAttempts", t.maxAttempts(),
                "created", t.created(),
                "due", t.due(),
                "stateChanged", t.stateChanged(),
                "claimedAt", t.claimedAt(),
                "leaseUntil", t.leaseUntil(),
                "workerId", t.workerId(),
                "account", t.account(),
                "headers", t.headers(),
                "uniqueKey", t.uniqueKey(),
                "scheduleId", t.scheduleId(),
                "retryOf", text(t.retryOf()),
                "startBy", t.startBy(),
                "maxRun", text(t.maxRun()),
                "cancelRequestedAt", t.cancelRequestedAt(),
                "cancelReason", t.cancelReason(),
                "progress", t.progress(),
                "steps", steps,
                "failures", failures);
    }

    static Map<String, Object> counts(StateCounts c) {
        return obj(
                "QUEUED", c.queued(),
                "SCHEDULED", c.scheduled(),
                "LEASED", c.leased(),
                "SUCCEEDED", c.succeeded(),
                "DEAD", c.dead(),
                "CANCELLED", c.cancelled(),
                "EXPIRED", c.expired());
    }

    static StateCounts sum(StateCounts a, StateCounts b) {
        return new StateCounts(
                a.queued() + b.queued(),
                a.scheduled() + b.scheduled(),
                a.leased() + b.leased(),
                a.succeeded() + b.succeeded(),
                a.dead() + b.dead(),
                a.cancelled() + b.cancelled(),
                a.expired() + b.expired());
    }

    static Map<String, Object> queue(QueueInfo q, StateCounts counts) {
        return obj("id", q.id(), "name", q.name(), "paused", q.paused(), "counts", counts(counts));
    }

    static Map<String, Object> health(RunnerHealth h) {
        return obj(
                "status", h.status().name(),
                "workerId", h.workerId(),
                "running", h.running(),
                "lastClaimAt", h.lastClaimAt(),
                "lastMaintenanceAt", h.lastMaintenanceAt(),
                "lastStoreErrorAt", h.lastStoreErrorAt(),
                "lastStoreError", h.lastStoreError(),
                "issueCount", h.issueCount());
    }

    static Map<String, Object> schedule(ScheduleView s) {
        return obj(
                "id", s.id(),
                "event", s.event(),
                "queue", s.queue(),
                "account", s.account(),
                "cron", s.cron(),
                "zone", s.zone(),
                "misfire", s.misfire(),
                "fixedDelay", text(s.fixedDelay()),
                "overlapSkip", s.overlapSkip(),
                "paused", s.paused(),
                "nextFire", s.nextFire(),
                "maxAttempts", s.maxAttempts());
    }

    static Map<String, Object> worker(WorkerInfo w) {
        return obj(
                "id", w.id(),
                "label", w.label(),
                "startedAt", w.startedAt(),
                "lastSeen", w.lastSeen(),
                "running", w.running());
    }

    static Map<String, Object> issue(Issue i) {
        return obj("at", i.at(), "kind", i.kind(), "detail", i.detail(), "taskId", text(i.taskId()));
    }

    static <T> List<Object> list(List<T> items, Function<T, Map<String, Object>> mapper) {
        var out = new ArrayList<Object>(items.size());
        for (var item : items) {
            out.add(mapper.apply(item));
        }
        return out;
    }
}
