package today.bonfire.oss.bth4j;

import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

/**
 * A kind of task: its name (stored with every task and used to pick the processor), its payload class and
 * how the payload is encoded. Declare one constant per task kind and use it for both enqueueing and
 * registering the processor.
 * <p>
 * {@code payloadVersion} is stored with each task; raise it when the payload shape changes, so a processor
 * can read old tasks differently.
 *
 * @param <T> the payload type; {@link Void} for a task without payload
 */
public final class TaskType<T> {

    private final String name;
    private final Class<T> payloadClass;
    private final PayloadCodec<T> codec;
    private final int payloadVersion;
    private final boolean interruptOnCancel;

    private TaskType(String name, Class<T> payloadClass, PayloadCodec<T> codec, int payloadVersion) {
        this(name, payloadClass, codec, payloadVersion, false);
    }

    private TaskType(
            String name, Class<T> payloadClass, PayloadCodec<T> codec, int payloadVersion, boolean interruptOnCancel) {
        if (StringUtils.isBlank(name) || name.length() > 128) {
            throw new TaskConfigurationError("Task type name must be 1 to 128 characters: " + name);
        }
        if (payloadVersion < 1) {
            throw new TaskConfigurationError("payloadVersion must be at least 1");
        }
        this.name = name;
        this.payloadClass = payloadClass;
        this.codec = codec;
        this.payloadVersion = payloadVersion;
        this.interruptOnCancel = interruptOnCancel;
    }

    /** A task type whose payload is written as JSON by the configured mapper. */
    public static <T> TaskType<T> of(String name, Class<T> payloadClass) {
        return new TaskType<>(name, payloadClass, null, 1);
    }

    /** A task type with its own codec. */
    public static <T> TaskType<T> of(String name, PayloadCodec<T> codec) {
        return new TaskType<>(name, null, codec, 1);
    }

    /** A task type without payload. */
    public static TaskType<Void> of(String name) {
        return new TaskType<>(name, Void.class, null, 1);
    }

    public TaskType<T> payloadVersion(int version) {
        return new TaskType<>(name, payloadClass, codec, version, interruptOnCancel);
    }

    /**
     * Interrupts the processor's thread when a cancel request arrives or the task's maximum run time passes,
     * so blocking calls and sleeps wake up. Off by default: the processor must then poll
     * {@link TaskContext#checkCancelled()}.
     */
    public TaskType<T> interruptOnCancel() {
        return new TaskType<>(name, payloadClass, codec, payloadVersion, true);
    }

    public boolean isInterruptOnCancel() {
        return interruptOnCancel;
    }

    public String name() {
        return name;
    }

    public int payloadVersion() {
        return payloadVersion;
    }

    /** @return the payload class, or null when a codec is used */
    public Class<T> payloadClass() {
        return payloadClass;
    }

    /** @return the codec, or null when the JSON mapper is used */
    public PayloadCodec<T> codec() {
        return codec;
    }

    public boolean hasPayload() {
        return payloadClass != Void.class;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TaskType<?> t && t.name.equals(name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return name;
    }
}
