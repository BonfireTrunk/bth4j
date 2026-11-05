package today.bonfire.oss.bth4j.spi;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * What a worker asks for in {@link ExecutionStore#claim}.
 *
 * @param queueIds      queues in order of preference; a later queue is read only when the earlier ones could
 *                      not fill the count
 * @param count         at most this many tasks
 * @param lease         how long each lease lasts unless renewed
 * @param workerId      the claiming worker, recorded with each task
 * @param knownTypeIds only tasks of these types are claimed; null means every type
 */
public record ClaimRequest(
        List<Integer> queueIds, int count, Duration lease, int workerId, Set<Integer> knownTypeIds) {}
