package today.bonfire.oss.bth4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

/**
 * The queues of an application: every queue a task may be enqueued to, and the one a task goes to when its
 * request names none. Give the same value to every {@link TaskClient} and {@link BackgroundRunner} of the
 * application, so that a producer and a runner cannot disagree about the default queue. A runner processes all
 * of them unless {@link BackgroundRunner.Builder#processQueues} names the ones it takes.
 * <p>
 * An application that configures nothing has the one queue {@link #GENERAL}.
 */
public final class Queues {

    /** The name of the queue an application has when it configures none. */
    public static final String GENERAL = "general";

    private final List<String> names;
    private final String defaultQueue;

    private Queues(List<String> names, String defaultQueue) {
        this.names = names;
        this.defaultQueue = defaultQueue;
    }

    /**
     * @param first the default queue, until {@link #withDefault} names another
     * @param more  the other queues; each name is 1 to 128 characters, a name given twice counts once
     */
    public static Queues of(String first, String... more) {
        if (more == null) throw new TaskConfigurationException("queue names cannot be null");
        var all = new ArrayList<String>(more.length + 1);
        all.add(first);
        all.addAll(Arrays.asList(more));
        return of(all);
    }

    /** The queues in the order given; the first is the default queue until {@link #withDefault} names another. */
    public static Queues of(List<String> names) {
        if (names == null || names.isEmpty()) {
            throw new TaskConfigurationException("at least one queue is required");
        }
        names.forEach(QueuesHolder::requireName);
        var distinct = List.copyOf(new LinkedHashSet<>(names));
        return new Queues(distinct, distinct.getFirst());
    }

    /** @return these queues with {@code queue}, which must be one of them, as the default queue */
    public Queues withDefault(String queue) {
        if (queue == null || !names.contains(queue)) {
            throw new TaskConfigurationException("the default queue must be one of the queues");
        }
        return new Queues(names, queue);
    }

    /** @return every queue, in the order given */
    public List<String> names() {
        return names;
    }

    /** @return the queue of a task whose request names none */
    public String defaultQueue() {
        return defaultQueue;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Queues other && names.equals(other.names) && defaultQueue.equals(other.defaultQueue);
    }

    @Override
    public int hashCode() {
        return 31 * names.hashCode() + defaultQueue.hashCode();
    }

    @Override
    public String toString() {
        return "Queues[names=" + names + ", defaultQueue=" + defaultQueue + ']';
    }
}
