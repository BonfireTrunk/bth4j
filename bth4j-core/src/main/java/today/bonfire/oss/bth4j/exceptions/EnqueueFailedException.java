package today.bonfire.oss.bth4j.exceptions;

import java.util.List;

/**
 * The store could not be reached while storing tasks, so the caller does not know whether they were
 * stored. {@link #ids()} holds the id every request of the call was given, in request order; enqueueing the
 * same requests again with those ids set is safe, the store answers accepted for a task it already has.
 * The guarantee holds while the task is retained: a task that ran and was removed by a zero retention is
 * stored again.
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
                "Enqueue of " + ids.size() + " task(s) starting at " + ids.getFirst() + " failed: "
                        + cause.getMessage(),
                cause);
        this.ids = List.copyOf(ids);
        this.stored = stored;
    }

    /** @return the id of the first (or only) task of the call */
    public long id() {
        return ids.getFirst();
    }

    /** @return the id of every task of the call, in request order */
    public List<Long> ids() {
        return ids;
    }

    /** @return how many tasks, counted from the start, were confirmed stored before the failure */
    public int stored() {
        return stored;
    }
}
