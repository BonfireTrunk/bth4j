package today.bonfire.oss.bth4j.spi;

import today.bonfire.oss.bth4j.Misfire;

/**
 * A recurring definition as the store keeps it. Each occurrence becomes a task with the definition's type,
 * queue, account, payload, maximum attempts and maximum run time.
 *
 * @param cron         five-field expression, or null for a fixed-delay definition
 * @param zone         zone id the cron expression is evaluated in; null means UTC
 * @param misfire      {@code SKIP}, {@code FIRE_ONCE_NOW} or {@code CATCH_UP}; null means FIRE_ONCE_NOW
 * @param catchUpMaxFires most occurrences a CATCH_UP misfire may create
 * @param fixedDelayMs delay between the end of one occurrence and the next, or null for cron
 * @param overlapSkip  true gives each occurrence the unique key {@code sched:<scheduleId>} so that one is
 *                     skipped while the previous one is not finished
 * @param paused       a paused definition is left out of {@link ScheduleStore#dueRecurring}
 * @param maxRunTimeMs     maximum run time of each occurrence's attempts, or null for the runner's default
 * @param revision     assigned by {@link ScheduleStore#putRecurring}, kept when the put changes nothing; a commit
 *                     for an older revision is rejected
 */
public record RecurringDefinition(
        String scheduleId,
        int typeId,
        int payloadVersion,
        int queueId,
        String account,
        String cron,
        String zone,
        String misfire,
        int catchUpMaxFires,
        Long fixedDelayMs,
        boolean overlapSkip,
        String encodedPayload,
        int maxAttempts,
        Long maxRunTimeMs,
        boolean paused,
        long revision) {

    public static final String MISFIRE_SKIP = "SKIP";
    public static final String MISFIRE_FIRE_ONCE_NOW = "FIRE_ONCE_NOW";
    public static final String MISFIRE_CATCH_UP = "CATCH_UP";

    /** @return the stored text of the policy: {@code SKIP}, {@code FIRE_ONCE_NOW} or {@code CATCH_UP} */
    public static String misfireCode(Misfire misfire) {
        return switch (misfire) {
            case Misfire.Skip s -> MISFIRE_SKIP;
            case Misfire.FireOnceNow f -> MISFIRE_FIRE_ONCE_NOW;
            case Misfire.CatchUp c -> MISFIRE_CATCH_UP;
        };
    }

    /** @return the stored catch-up count of the policy; zero unless it is a catch-up */
    public static int misfireMaxFires(Misfire misfire) {
        return misfire instanceof Misfire.CatchUp c ? c.maxFires() : 0;
    }

    /** @return the policy for a stored text; null and any unknown text mean {@link Misfire#FIRE_ONCE_NOW} */
    public static Misfire misfire(String code, int catchUpMaxFires) {
        if (code == null) return Misfire.FIRE_ONCE_NOW;
        return switch (code) {
            case MISFIRE_SKIP -> Misfire.SKIP;
            case MISFIRE_CATCH_UP -> Misfire.catchUp(catchUpMaxFires);
            default -> Misfire.FIRE_ONCE_NOW;
        };
    }

    /** A cron definition in UTC with the default misfire handling and no overlap protection. */
    public RecurringDefinition(
            String scheduleId,
            int typeId,
            int payloadVersion,
            int queueId,
            String account,
            String cron,
            String encodedPayload,
            int maxAttempts,
            long revision) {
        this(
                scheduleId,
                typeId,
                payloadVersion,
                queueId,
                account,
                cron,
                null,
                null,
                0,
                null,
                false,
                encodedPayload,
                maxAttempts,
                null,
                false,
                revision);
    }

    public RecurringDefinition withRevision(long revision) {
        return new RecurringDefinition(
                scheduleId,
                typeId,
                payloadVersion,
                queueId,
                account,
                cron,
                zone,
                misfire,
                catchUpMaxFires,
                fixedDelayMs,
                overlapSkip,
                encodedPayload,
                maxAttempts,
                maxRunTimeMs,
                paused,
                revision);
    }

    public RecurringDefinition withPaused(boolean paused) {
        return new RecurringDefinition(
                scheduleId,
                typeId,
                payloadVersion,
                queueId,
                account,
                cron,
                zone,
                misfire,
                catchUpMaxFires,
                fixedDelayMs,
                overlapSkip,
                encodedPayload,
                maxAttempts,
                maxRunTimeMs,
                paused,
                revision);
    }

    public boolean isFixedDelay() {
        return fixedDelayMs != null;
    }
}
