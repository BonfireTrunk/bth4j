package today.bonfire.oss.bth4j.spi;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * What a runner asks for in {@link ExecutionStore#claim}.
 *
 * @param grants        queue shares and caps in order of preference
 * @param count         at most this many tasks
 * @param lease         how long each lease lasts unless renewed
 * @param runnerId      the claiming runner, recorded with each task
 * @param knownTypeIds only tasks of these types are claimed; null means every type
 */
public record ClaimRequest(
        List<QueueGrant> grants, int count, Duration lease, int runnerId, Set<Integer> knownTypeIds) {
    public ClaimRequest {
        grants = List.copyOf(grants);
        if (grants.isEmpty()) throw new IllegalArgumentException("grants must not be empty");
        var queues = new HashSet<Integer>();
        for (var grant : grants) {
            if (!queues.add(grant.queueId())) throw new IllegalArgumentException("duplicate queue grant");
        }
        if (knownTypeIds != null) knownTypeIds = Set.copyOf(knownTypeIds);
    }
}
