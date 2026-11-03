package today.bonfire.oss.bth4j.exceptions;

import java.util.List;

/**
 * The store could not be reached while storing tasks, so the caller does not know whether they were
 * stored. {@link #ids()} holds the id every request of the call was given, in request order; enqueueing the
 * same requests again with those ids set is safe, the store answers accepted for a task it already has.
 * The guarantee holds while the task is retained: a task that ran and was removed by a zero retention is
 * stored again. When the store failed before the ids were fixed (reserving a block of ids, checking a caller's
 * id), {@link #ids()} is empty or holds only the ids the caller set itself: nothing was written, and the
 * caller retries as before, without ids.
 */
public class EnqueueFailedException extends RuntimeException {
    private final List<Long> ids;
    private final int stored;

    /**
     * @param ids    the id of every request in the failed call, in order
     * @param stored how many of them were confirmed stored before the failure
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
     * @return the id of the first (or only) task of the call
     * @throws java.util.NoSuchElementException when {@link #ids()} is empty
     */
    public long id() {
        return ids.getFirst();
    }

    /** @return the id of every task of the call, in request order; empty when none was fixed before the failure */
    public List<Long> ids() {
        return ids;
    }

    /** @return how many tasks, counted from the start, were confirmed stored before the failure */
    public int stored() {
        return stored;
    }
}
