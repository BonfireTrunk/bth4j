package today.bonfire.oss.bth4j;

import java.time.Instant;

/**
 * Something an operator should look at, kept in memory by the runner that saw it (bounded, newest last).
 *
 * @param kind   {@code unknown-event}, {@code store-error}, {@code lease-lost}, {@code retry-policy},
 *               {@code executor-rejected}, {@code schedule-misfire}, {@code schedule-overlap}, {@code schedule-paused}
 * @param taskId the task concerned, or null
 */
public record Issue(Instant at, String kind, String detail, Long taskId) {}
