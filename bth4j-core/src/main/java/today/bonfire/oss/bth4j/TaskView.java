package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/** A task as applications see it: the stored record with queue and type names resolved. */
public final class TaskView {

    private final TaskRecord record;
    private final String queue;
    private final String type;

    public TaskView(TaskRecord record, String queue, String type) {
        this.record = record;
        this.queue = queue;
        this.type = type;
    }

    public TaskRecord record() {
        return record;
    }

    public long id() {
        return record.id();
    }

    public int revision() {
        return record.revision();
    }

    public String queue() {
        return queue;
    }

    /** @return the task type name */
    public String type() {
        return type;
    }

    public int payloadVersion() {
        return record.payloadVersion();
    }

    public TaskState state() {
        return record.state();
    }

    public int attempts() {
        return record.attempts();
    }

    public int maxAttempts() {
        return record.maxAttempts();
    }

    public Instant createdAt() {
        return record.createdAt();
    }

    public Instant due() {
        return record.due();
    }

    public Instant stateChangedAt() {
        return record.stateChangedAt();
    }

    /** @return the encoded payload, or null */
    public String encodedPayload() {
        return record.encodedPayload();
    }

    public Integer workerId() {
        return record.workerId();
    }

    public Instant leaseUntil() {
        return record.leaseUntil();
    }

    public Instant claimedAt() {
        return record.claimedAt();
    }

    public String account() {
        return record.account();
    }

    public Map<String, String> headers() {
        return record.headers() == null ? Map.of() : record.headers();
    }

    public String uniqueKey() {
        return record.uniqueKey();
    }

    /** @return how long the unique key stays taken after the task ends, or null when it is released at once */
    public Duration keyKeep() {
        return record.keyKeep();
    }

    /** @return when the key of this finished task is released, or null when the task holds no key that way */
    public Instant keyUntil() {
        return record.keyUntil();
    }

    /** @return the name of the retry policy the request chose, or null for the runner's default */
    public String retryPolicy() {
        return record.retryPolicy();
    }

    public String scheduleId() {
        return record.scheduleId();
    }

    public Long retryOf() {
        return record.retryOf();
    }

    public Instant startBy() {
        return record.startBy();
    }

    public Duration maxRunTime() {
        return record.maxRunTime();
    }

    public Instant cancelRequestedAt() {
        return record.cancelRequestedAt();
    }

    public String cancelReason() {
        return record.cancelReason();
    }

    public String progress() {
        return record.progress();
    }

    public Map<String, StepResult> steps() {
        return record.steps() == null ? Map.of() : record.steps();
    }

    public List<FailureInfo> failures() {
        return record.failures() == null ? List.of() : record.failures();
    }

    public FailureInfo lastFailure() {
        return record.lastFailure();
    }

    @Override
    public String toString() {
        return "Task[#" + record.id() + ", " + type + ", queue=" + queue + ", " + record.state() + ", attempts="
                + record.attempts() + "/" + record.maxAttempts() + ']';
    }
}
