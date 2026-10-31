package today.bonfire.oss.bth4j.spi;

import java.time.Instant;

/** A stored definition with its next fire time. */
public record RecurringSchedule(RecurringDefinition definition, Instant nextFire) {}
