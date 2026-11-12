package today.bonfire.oss.bth4j.spi;

import java.time.Instant;

/**
 * A schedule that is due, together with the fire time the store has for it.
 *
 * @param triggered true when an operator set this fire time with {@link ScheduleStore#triggerSchedule}; such a
 *     fire is not a missed one, however late it is taken up
 */
public record DueSchedule(String scheduleId, Instant fireAt, StoredSchedule definition, boolean triggered) {}
