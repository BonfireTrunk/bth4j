package today.bonfire.oss.bth4j.spi;

import java.time.Instant;

/** A stored schedule with its next fire time, as {@link ScheduleStore#schedules()} lists it. */
public record ScheduleEntry(StoredSchedule definition, Instant nextFireAt) {}
