package today.bonfire.oss.bth4j.exceptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import today.bonfire.oss.bth4j.EnqueueResult;
import today.bonfire.oss.bth4j.TaskRequest;

/**
 * An enqueue could not complete. An empty {@link #ids()} means nothing was written by this call: resend
 * the same requests unchanged, keeping any ids the caller set. Otherwise {@link #ids()} holds every
 * request's last assigned id in request order. For positions in {@link #notStored()}, the id may belong
 * to another task; do not reuse it. {@link #settled()} holds the results of the chunks that completed, and
 * {@link #retryRequests(List)} prepares the rest of a failed batch for retry.
 * <p>
 * Retrying the same requests, encoded identically, is idempotent while the tasks are retained. A task that
 * ran and was removed can be stored again. Caller-id reuse still requires caller correction.
 */
public class EnqueueFailedException extends RuntimeException {
    private final List<Long> ids;
    private final List<EnqueueResult> settled;
    private final Set<Integer> notStored;

    /**
     * @param ids     the last assigned id of every request, in order; empty to indicate that the call wrote nothing
     * @param settled the results of the requests in completed chunks before the failure, in order; empty for a
     *                single enqueue
     */
    public EnqueueFailedException(List<Long> ids, List<EnqueueResult> settled, Throwable cause) {
        this(ids, settled, Set.of(), cause);
    }

    /**
     * @param ids       the last assigned id of every request, in order; empty when the call wrote nothing
     * @param settled   the results of the requests in completed chunks before the failure, in order
     * @param notStored zero-based positions of generated-id requests known not to have been written
     * @throws IllegalArgumentException when there are more settled results than ids
     */
    public EnqueueFailedException(
            List<Long> ids, List<EnqueueResult> settled, Set<Integer> notStored, Throwable cause) {
        super(
                "Enqueue of " + ids.size() + " task(s)" + (ids.isEmpty() ? "" : " starting at " + ids.getFirst())
                        + " failed: " + cause.getMessage(),
                cause);
        if (settled.size() > ids.size()) throw new IllegalArgumentException("more settled results than ids");
        this.ids = List.copyOf(ids);
        this.settled = List.copyOf(settled);
        this.notStored = Set.copyOf(notStored);
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

    /**
     * @return every request's last assigned id, in order; empty means resend the same requests unchanged.
     *         For positions in {@link #notStored()}, the id may belong to another task; do not reuse it.
     */
    public List<Long> ids() {
        return ids;
    }

    /**
     * @return immutable zero-based positions of generated-id requests this call knows it did not write;
     *         caller-set ids are never included. Other positions may already be stored.
     */
    public Set<Integer> notStored() {
        return notStored;
    }

    /**
     * Prepares the requests that did not settle, in order, for retry: those from position {@link #storedCount()}
     * on, so that the settled ones are not sent again. A settled {@code Duplicate} sent again could be stored
     * once its key's holder has finished, running work the first call had deduplicated. Requests in
     * {@link #notStored()} are returned unchanged so the client generates new ids; other positions get their
     * last assigned id. When {@link #ids()} is empty, every request is returned unchanged. Caller-id reuse
     * errors still require caller correction. Neither the input list nor its requests are modified. The results
     * of the whole batch are {@link #settled()} followed by the results of the retry.
     *
     * @param requests the requests of the failed call, in the same order
     * @return an immutable list ready for {@code TaskClient.enqueueAll}
     * @throws IllegalArgumentException when nonempty {@link #ids()} and the requests have different sizes
     */
    public List<TaskRequest<?>> retryRequests(List<? extends TaskRequest<?>> requests) {
        if (ids.isEmpty()) return List.copyOf(requests);
        if (requests.size() != ids.size()) {
            throw new IllegalArgumentException("requests must have the same size and order as the failed batch");
        }
        var retry = new ArrayList<TaskRequest<?>>(requests.size() - settled.size());
        for (int i = settled.size(); i < requests.size(); i++) {
            var request = requests.get(i);
            retry.add(notStored.contains(i) ? request : request.withId(ids.get(i)));
        }
        return List.copyOf(retry);
    }

    /**
     * @return the results of the requests in completed chunks, in request order, including duplicate-key results;
     *         the failing chunk may also have stored tasks
     */
    public List<EnqueueResult> settled() {
        return settled;
    }

    /** @return the number of {@link #settled()} requests, the position of the first request to retry */
    public int storedCount() {
        return settled.size();
    }
}
