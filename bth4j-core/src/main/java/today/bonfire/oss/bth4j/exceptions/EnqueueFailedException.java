package today.bonfire.oss.bth4j.exceptions;

import java.util.List;

/**
 * An enqueue could not complete. An empty {@link #ids()} means nothing was written by this call: resend
 * the same requests unchanged, keeping any ids the caller set. Otherwise {@link #ids()} holds every request's
 * last assigned id in request order; tasks may already be stored, so a retry must set those ids.
 * <p>
 * Retrying the same requests, encoded identically, is idempotent while the tasks are retained. A task that
 * ran and was removed can be stored again. Collision exhaustion carries newly reserved ids this call did not
 * write (not checked to be free) for the occupied positions and any generated positions in unattempted chunks.
 * If replacement reservation fails, the replacements overlap a caller id, or the cause reports concurrent
 * caller-id reuse, resolve the collisions before retrying: the retained ids
 * can belong to other tasks, even with identical content.
 */
public class EnqueueFailedException extends RuntimeException {
    private final List<Long> ids;
    private final int stored;

    /**
     * @param ids    the last assigned id of every request, in order; empty to indicate that the call wrote nothing
     * @param stored the number of requests in completed chunks before the failure; zero for a single enqueue
     */
    public EnqueueFailedException(List<Long> ids, int stored, Throwable cause) {
        super(
                "Enqueue of " + ids.size() + " task(s)" + (ids.isEmpty() ? "" : " starting at " + ids.getFirst())
                        + " failed: " + cause.getMessage(),
                cause);
        this.ids = List.copyOf(ids);
        this.stored = stored;
    }

    /**
     * Check {@link #ids()} for emptiness before calling: a failure before reservation may carry no id.
     *
     * @return the id of the first (or only) task of the call
     * @throws java.util.NoSuchElementException when {@link #ids()} is empty
     */
    public long id() {
        return ids.getFirst();
    }

    /** @return every request's last assigned id, in order; empty means resend the same requests unchanged */
    public List<Long> ids() {
        return ids;
    }

    /** @return the number of requests in completed chunks, including duplicate-key results; the failing chunk may also have stored tasks */
    public int stored() {
        return stored;
    }
}
