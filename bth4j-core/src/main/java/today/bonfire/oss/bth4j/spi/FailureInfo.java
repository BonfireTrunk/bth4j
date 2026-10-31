package today.bonfire.oss.bth4j.spi;

import java.time.Instant;

/**
 * One failed attempt, as kept with the task.
 *
 * @param attempt  which attempt failed, starting at 1
 * @param at       when the failure was recorded
 * @param type     exception class name, or a category such as {@code lease-expired}
 * @param message  exception message, bounded by the client
 * @param stack    stack trace text, bounded by the client; may be null
 */
public record FailureInfo(int attempt, Instant at, String type, String message, String stack) {}
