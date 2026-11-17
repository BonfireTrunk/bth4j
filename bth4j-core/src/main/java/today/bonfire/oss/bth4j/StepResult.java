package today.bonfire.oss.bth4j;

import java.time.Instant;

/**
 * A completed step of a task.
 *
 * @param at     when the step finished
 * @param result the encoded result, or null for a step without one
 */
public record StepResult(Instant at, String result) {}
