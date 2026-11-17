package today.bonfire.oss.bth4j;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * A registered runner as applications and the admin API see it.
 *
 * @param label      the runner's {@code runnerLabel}
 * @param lastSeenAt the time of its last heartbeat, on the store's clock
 * @param running    how many tasks it was running at that heartbeat
 * @param accepting  false while the runner is quiet or draining
 * @param queues     the names of the queues it processes, sorted; a queue whose name the store no longer has is
 *                   listed as {@code #<id>}
 * @param types      the names of the task types it has processors for, sorted, with {@code #<id>} likewise
 */
public record RunnerView(
        int id,
        String label,
        Instant startedAt,
        Instant lastSeenAt,
        int running,
        boolean accepting,
        Set<String> queues,
        Set<String> types) {
    public RunnerView {
        queues = Collections.unmodifiableSet(new TreeSet<>(Objects.requireNonNull(queues, "queues")));
        types = Collections.unmodifiableSet(new TreeSet<>(Objects.requireNonNull(types, "types")));
    }
}
