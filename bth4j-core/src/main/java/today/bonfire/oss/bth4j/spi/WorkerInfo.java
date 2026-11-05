package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/** A registered worker with the queues and task types it processes. */
public record WorkerInfo(
        int id,
        String label,
        Instant startedAt,
        Instant lastSeen,
        int running,
        Set<Integer> queueIds,
        Set<Integer> typeIds) {
    public WorkerInfo {
        queueIds = Set.copyOf(Objects.requireNonNull(queueIds, "queueIds"));
        typeIds = Set.copyOf(Objects.requireNonNull(typeIds, "typeIds"));
    }
}
