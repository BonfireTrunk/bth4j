package today.bonfire.oss.bth4j.store;

import java.time.Instant;

/**
 * Result of {@link TaskStore#promote}.
 *
 * @param moved   how many tasks were moved into their queues
 * @param nextDue due time of the earliest task still scheduled, or null when there is none
 */
public record PromoteResult(int moved, Instant nextDue) {}
