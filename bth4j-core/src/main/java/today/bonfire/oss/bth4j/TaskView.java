package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

/** A task as applications see it: the stored record with queue and event names resolved. */
public final class TaskView {

    private final TaskRecord record;
    private final String queue;
    private final String event;

    public TaskView(TaskRecord record, String queue, String event) {
        this.record = record;
        this.queue = queue;
        this.event = event;
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
    public String event() {
        return event;
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

    public Instant created() {
        return record.created();
    }

    public Instant due() {
        return record.due();
    }

    public Instant stateChanged() {
        return record.stateChanged();
    }

    /** @return the encoded payload, or null */
    public String data() {
        return record.data();
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

    public String scheduleId() {
        return record.scheduleId();
    }

    public Long retryOf() {
        return record.retryOf();
    }

    public Instant startBy() {
        return record.startBy();
    }

    public Duration maxRun() {
        return record.maxRun();
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
        return "Task[#" + record.id() + ", " + event + ", queue=" + queue + ", " + record.state() + ", attempts="
                + record.attempts() + "/" + record.maxAttempts() + ']';
    }
}
