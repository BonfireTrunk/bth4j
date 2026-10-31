package today.bonfire.oss.bth4j.spi;

/** The states a task moves through. The code is what stores persist. */
public enum TaskState {
    /** waiting in its queue, runnable now */
    QUEUED(0),
    /** waiting for its due time */
    SCHEDULED(1),
    /** claimed by a worker that holds the current revision */
    LEASED(2),
    /** finished successfully; kept for the succeeded retention */
    SUCCEEDED(3),
    /** attempts exhausted or retry refused; kept for the dead retention */
    DEAD(4),
    /** ended by a cancel request; kept for the cancelled retention */
    CANCELLED(5),
    /** its start-by deadline passed before it ran; kept for the cancelled retention */
    EXPIRED(6);

    private static final TaskState[] BY_CODE = values();

    private final int code;

    TaskState(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public boolean isTerminal() {
        return code >= SUCCEEDED.code;
    }

    public static TaskState fromCode(int code) {
        if (code < 0 || code >= BY_CODE.length) {
            throw new IllegalArgumentException("Unknown task state code " + code);
        }
        return BY_CODE[code];
    }
}
