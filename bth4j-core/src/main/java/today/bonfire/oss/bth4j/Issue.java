package today.bonfire.oss.bth4j;

import java.time.Instant;

/**
 * Something an operator should look at, kept in memory by the runner that saw it (bounded, newest last).
 *
 * @param kind   what happened; {@link Kind#text()} is the text the admin HTTP API sends
 * @param taskId the task concerned, or null
 */
public record Issue(Instant at, Kind kind, String detail, Long taskId) {

    /** The kinds of issue. New kinds may be added in a minor release. */
    public enum Kind {
        /** A claimed task has no processor on this runner and was deferred. */
        UNKNOWN_TYPE("unknown-type"),
        /**
         * A store call failed; repeats of the same error text are one issue whose detail carries the count and
         * whose time is the latest.
         */
        STORE_ERROR("store-error"),
        /** A lease was not renewed or an outcome was refused because the task was taken over. */
        LEASE_LOST("lease-lost"),
        /** A task names a retry policy this runner does not have, or a policy threw. */
        RETRY_POLICY("retry-policy"),
        /** The executor refused a claimed task; it went back to its queue. */
        EXECUTOR_REJECTED("executor-rejected"),
        /**
         * An attempt ran past its maximum run time and did not stop; its lease is no longer renewed and its slots
         * stay held until it ends.
         */
        ATTEMPT_ABANDONED("attempt-abandoned"),
        /** A task with a stored value that cannot be read ended {@code DEAD}. */
        CORRUPT_TASK("corrupt-task"),
        /** The adaptive executor could not read the CPU or memory signal. */
        RESOURCE_SIGNAL_UNAVAILABLE("resource-signal-unavailable"),
        /** A recurring fire was late and its misfire policy was applied. */
        SCHEDULE_MISFIRE("schedule-misfire"),
        /** A recurring fire was skipped because the previous occurrence is still running. */
        SCHEDULE_OVERLAP("schedule-overlap"),
        /** A fixed-delay definition was paused because its occurrence did not succeed. */
        SCHEDULE_PAUSED("schedule-paused"),
        /** A fixed-delay definition was not re-armed because its delay header cannot be read. */
        SCHEDULE_REARM("schedule-rearm"),
        /**
         * A recurring definition cannot be evaluated (for example an invalid cron expression or a removed time
         * zone); its next fire was moved ahead so that it does not block the others.
         */
        SCHEDULE_BROKEN("schedule-broken");

        private final String text;

        Kind(String text) {
            this.text = text;
        }

        /** @return the fixed text of this kind, as the admin HTTP API sends it */
        public String text() {
            return text;
        }
    }
}
