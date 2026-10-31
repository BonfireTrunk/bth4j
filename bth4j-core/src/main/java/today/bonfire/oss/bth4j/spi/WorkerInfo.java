package today.bonfire.oss.bth4j.spi;

import java.time.Instant;

public record WorkerInfo(int id, String label, Instant startedAt, Instant lastSeen, int running) {}
