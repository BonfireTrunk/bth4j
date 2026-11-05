package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import java.util.Set;

/** Worker coverage is null when it was not recorded, for example by an older worker. */
public record WorkerInfo(
        int id,
        String label,
        Instant startedAt,
        Instant lastSeen,
        int running,
        Set<Integer> queueIds,
        Set<Integer> typeIds) {
    public WorkerInfo {
        queueIds = queueIds == null ? null : Set.copyOf(queueIds);
        typeIds = typeIds == null ? null : Set.copyOf(typeIds);
    }
}
