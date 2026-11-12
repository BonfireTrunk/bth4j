package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * A task as applications see it: the stored task with queue and type names resolved. The library builds views
 * from the store; {@link #builder()} makes one by hand for a test of an observer, a retry policy or an
 * {@link AdminOps} fake.
 */
public final class TaskView {

    private final TaskRecord record;
    private final String queue;
    private final String type;

    TaskView(TaskRecord record, String queue, String type) {
        this.record = record;
        this.queue = queue;
        this.type = type;
    }

    /**
     * @return a builder for a view made by hand: a QUEUED task with payload version 1 and one attempt allowed;
     *         every other field that is not set is null, zero or empty
     */
    public static Builder builder() {
        return new Builder();
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

    public Integer runnerId() {
        return record.runnerId();
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

    /** Builds a {@link TaskView} by hand. */
    public static final class Builder {
        private final TaskRecord.Builder record = TaskRecord.builder();
        private String queue;
        private String type;

        private Builder() {}

        public Builder queue(String v) {
            queue = v;
            return this;
        }

        public Builder type(String v) {
            type = v;
            return this;
        }

        public Builder id(long v) {
            record.id(v);
            return this;
        }

        public Builder revision(int v) {
            record.revision(v);
            return this;
        }

        public Builder payloadVersion(int v) {
            record.payloadVersion(v);
            return this;
        }

        public Builder state(TaskState v) {
            record.state(v);
            return this;
        }

        public Builder attempts(int v) {
            record.attempts(v);
            return this;
        }

        public Builder maxAttempts(int v) {
            record.maxAttempts(v);
            return this;
        }

        public Builder createdAt(Instant v) {
            record.createdAt(v);
            return this;
        }

        public Builder due(Instant v) {
            record.due(v);
            return this;
        }

        public Builder stateChangedAt(Instant v) {
            record.stateChangedAt(v);
            return this;
        }

        public Builder encodedPayload(String v) {
            record.encodedPayload(v);
            return this;
        }

        public Builder runnerId(Integer v) {
            record.runnerId(v);
            return this;
        }

        public Builder leaseUntil(Instant v) {
            record.leaseUntil(v);
            return this;
        }

        public Builder claimedAt(Instant v) {
            record.claimedAt(v);
            return this;
        }

        public Builder account(String v) {
            record.account(v);
            return this;
        }

        public Builder headers(Map<String, String> v) {
            record.headers(v);
            return this;
        }

        public Builder uniqueKey(String v) {
            record.uniqueKey(v);
            return this;
        }

        public Builder keyKeep(Duration v) {
            record.keyKeep(v);
            return this;
        }

        public Builder keyUntil(Instant v) {
            record.keyUntil(v);
            return this;
        }

        public Builder retryPolicy(String v) {
            record.retryPolicy(v);
            return this;
        }

        public Builder scheduleId(String v) {
            record.scheduleId(v);
            return this;
        }

        public Builder retryOf(Long v) {
            record.retryOf(v);
            return this;
        }

        public Builder startBy(Instant v) {
            record.startBy(v);
            return this;
        }

        public Builder maxRunTime(Duration v) {
            record.maxRunTime(v);
            return this;
        }

        public Builder cancelRequestedAt(Instant v) {
            record.cancelRequestedAt(v);
            return this;
        }

        public Builder cancelReason(String v) {
            record.cancelReason(v);
            return this;
        }

        public Builder progress(String v) {
            record.progress(v);
            return this;
        }

        public Builder steps(Map<String, StepResult> v) {
            record.steps(v);
            return this;
        }

        public Builder failures(List<FailureInfo> v) {
            record.failures(v);
            return this;
        }

        public TaskView build() {
            return new TaskView(record.build(), queue, type);
        }
    }
}
