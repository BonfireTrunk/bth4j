package today.bonfire.oss.bth4j.store;

import java.time.Instant;

/** A recurring definition that is due, together with the fire time the store has for it. */
public record DueSchedule(String scheduleId, Instant fireAt, RecurringDefinition definition) {}
