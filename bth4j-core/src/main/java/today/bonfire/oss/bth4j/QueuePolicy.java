package today.bonfire.oss.bth4j;

import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

/**
 * How one runner shares its executor slots between queues.
 *
 * @param weight        share of the free slots this queue gets each claim cycle, relative to the other queues (default 1)
 * @param maxConcurrent most tasks of this queue running at once on this runner; 0 means no limit
 */
public record QueuePolicy(int weight, int maxConcurrent) {
    public static final QueuePolicy DEFAULT = new QueuePolicy(1, 0);

    public QueuePolicy {
        if (weight < 1) throw new TaskConfigurationError("weight must be at least 1");
        if (maxConcurrent < 0) throw new TaskConfigurationError("maxConcurrent cannot be negative");
    }

    public static QueuePolicy weight(int weight) {
        return new QueuePolicy(weight, 0);
    }

    public static QueuePolicy maxConcurrent(int maxConcurrent) {
        return new QueuePolicy(1, maxConcurrent);
    }
}
