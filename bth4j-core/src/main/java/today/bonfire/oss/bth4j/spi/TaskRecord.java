package today.bonfire.oss.bth4j.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * A task as the store keeps it. Fields that do not apply to the task's state are null. A store never
 * writes a null field (Redis omits the hash field, PostgreSQL stores SQL null).
 *
 * @param id                unique per namespace, allocated by the client from {@link TaskStore#allocateIds}
 * @param revision          +1 on every state change; never on renew, step or progress writes; the fence every
 *                          transition checks
 * @param queueId           see {@link TaskStore#queueId}
 * @param eventId           see {@link TaskStore#eventId}
 * @param payloadVersion    the task type's payload version when the task was enqueued
 * @param state             current state
 * @param attempts          how often the task was claimed
 * @param maxAttempts       after this many failed attempts the task is dead
 * @param created           when the client built the task (client clock)
 * @param due               when the task may run: the enqueue time for an immediate task, the due time for a
 *                          scheduled one, the retry time after a failure (store clock); never null once stored
 * @param stateChanged      when the current state was entered (store clock); the finish time of a terminal task
 * @param data              encoded payload; null when there is none or it was dropped on success
 * @param workerId          the worker that claimed the task; kept after it finished
 * @param leaseUntil        when the lease expires unless renewed; LEASED only
 * @param claimedAt         when the last attempt started
 * @param account           application-defined owner, or null
 * @param headers           application headers, or null
 * @param uniqueKey         the unique key while it is held, or null
 * @param keyKeep           how long the key stays held after the task ends; null releases it at the end
 * @param keyUntil          when the held key of a finished task is released
 * @param retryPolicy       name of the retry policy; null means the runner's default
 * @param scheduleId        the recurring definition this task is an occurrence of, or null
 * @param retryOf           the dead task this one was created from by an admin retry, or null
 * @param startBy           the task expires when it has not started by then, or null
 * @param maxRun            the task is failed when an attempt runs longer, or null
 * @param cancelRequestedAt when a cancel was requested, or null
 * @param cancelReason      why, or null
 * @param progress          free-form progress text set by the processor, or null
 * @param steps             completed steps by name, or null
 * @param failures          the last failures, oldest first, or null
 */
public record TaskRecord(
        long id,
        int revision,
        int queueId,
        int eventId,
        int payloadVersion,
        TaskState state,
        int attempts,
        int maxAttempts,
        Instant created,
        Instant due,
        Instant stateChanged,
        String data,
        Integer workerId,
        Instant leaseUntil,
        Instant claimedAt,
        String account,
        Map<String, String> headers,
        String uniqueKey,
        Duration keyKeep,
        Instant keyUntil,
        String retryPolicy,
        String scheduleId,
        Long retryOf,
        Instant startBy,
        Duration maxRun,
        Instant cancelRequestedAt,
        String cancelReason,
        String progress,
        Map<String, StepResult> steps,
        List<FailureInfo> failures) {

    public TaskRecord {
        headers = headers == null || headers.isEmpty() ? null : Map.copyOf(headers);
        steps = steps == null || steps.isEmpty() ? null : Map.copyOf(steps);
        failures = failures == null || failures.isEmpty() ? null : List.copyOf(failures);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        var b = new Builder();
        b.id = id;
        b.revision = revision;
        b.queueId = queueId;
        b.eventId = eventId;
        b.payloadVersion = payloadVersion;
        b.state = state;
        b.attempts = attempts;
        b.maxAttempts = maxAttempts;
        b.created = created;
        b.due = due;
        b.stateChanged = stateChanged;
        b.data = data;
        b.workerId = workerId;
        b.leaseUntil = leaseUntil;
        b.claimedAt = claimedAt;
        b.account = account;
        b.headers = headers;
        b.uniqueKey = uniqueKey;
        b.keyKeep = keyKeep;
        b.keyUntil = keyUntil;
        b.retryPolicy = retryPolicy;
        b.scheduleId = scheduleId;
        b.retryOf = retryOf;
        b.startBy = startBy;
        b.maxRun = maxRun;
        b.cancelRequestedAt = cancelRequestedAt;
        b.cancelReason = cancelReason;
        b.progress = progress;
        b.steps = steps;
        b.failures = failures;
        return b;
    }

    /** @return the last recorded failure, or null */
    public FailureInfo lastFailure() {
        return failures == null ? null : failures.get(failures.size() - 1);
    }

    public static final class Builder {
        private long id;
        private int revision;
        private int queueId;
        private int eventId;
        private int payloadVersion = 1;
        private TaskState state = TaskState.QUEUED;
        private int attempts;
        private int maxAttempts = 1;
        private Instant created;
        private Instant due;
        private Instant stateChanged;
        private String data;
        private Integer workerId;
        private Instant leaseUntil;
        private Instant claimedAt;
        private String account;
        private Map<String, String> headers;
        private String uniqueKey;
        private Duration keyKeep;
        private Instant keyUntil;
        private String retryPolicy;
        private String scheduleId;
        private Long retryOf;
        private Instant startBy;
        private Duration maxRun;
        private Instant cancelRequestedAt;
        private String cancelReason;
        private String progress;
        private Map<String, StepResult> steps;
        private List<FailureInfo> failures;

        private Builder() {}

        public Builder id(long v) {
            id = v;
            return this;
        }

        public Builder revision(int v) {
            revision = v;
            return this;
        }

        public Builder queueId(int v) {
            queueId = v;
            return this;
        }

        public Builder eventId(int v) {
            eventId = v;
            return this;
        }

        public Builder payloadVersion(int v) {
            payloadVersion = v;
            return this;
        }

        public Builder state(TaskState v) {
            state = v;
            return this;
        }

        public Builder attempts(int v) {
            attempts = v;
            return this;
        }

        public Builder maxAttempts(int v) {
            maxAttempts = v;
            return this;
        }

        public Builder created(Instant v) {
            created = v;
            return this;
        }

        public Builder due(Instant v) {
            due = v;
            return this;
        }

        public Builder stateChanged(Instant v) {
            stateChanged = v;
            return this;
        }

        public Builder data(String v) {
            data = v;
            return this;
        }

        public Builder workerId(Integer v) {
            workerId = v;
            return this;
        }

        public Builder leaseUntil(Instant v) {
            leaseUntil = v;
            return this;
        }

        public Builder claimedAt(Instant v) {
            claimedAt = v;
            return this;
        }

        public Builder account(String v) {
            account = v;
            return this;
        }

        public Builder headers(Map<String, String> v) {
            headers = v;
            return this;
        }

        public Builder uniqueKey(String v) {
            uniqueKey = v;
            return this;
        }

        public Builder keyKeep(Duration v) {
            keyKeep = v;
            return this;
        }

        public Builder keyUntil(Instant v) {
            keyUntil = v;
            return this;
        }

        public Builder retryPolicy(String v) {
            retryPolicy = v;
            return this;
        }

        public Builder scheduleId(String v) {
            scheduleId = v;
            return this;
        }

        public Builder retryOf(Long v) {
            retryOf = v;
            return this;
        }

        public Builder startBy(Instant v) {
            startBy = v;
            return this;
        }

        public Builder maxRun(Duration v) {
            maxRun = v;
            return this;
        }

        public Builder cancelRequestedAt(Instant v) {
            cancelRequestedAt = v;
            return this;
        }

        public Builder cancelReason(String v) {
            cancelReason = v;
            return this;
        }

        public Builder progress(String v) {
            progress = v;
            return this;
        }

        public Builder steps(Map<String, StepResult> v) {
            steps = v;
            return this;
        }

        public Builder failures(List<FailureInfo> v) {
            failures = v;
            return this;
        }

        public TaskRecord build() {
            return new TaskRecord(
                    id,
                    revision,
                    queueId,
                    eventId,
                    payloadVersion,
                    state,
                    attempts,
                    maxAttempts,
                    created,
                    due,
                    stateChanged,
                    data,
                    workerId,
                    leaseUntil,
                    claimedAt,
                    account,
                    headers,
                    uniqueKey,
                    keyKeep,
                    keyUntil,
                    retryPolicy,
                    scheduleId,
                    retryOf,
                    startBy,
                    maxRun,
                    cancelRequestedAt,
                    cancelReason,
                    progress,
                    steps,
                    failures);
        }
    }
}
