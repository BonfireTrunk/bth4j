package today.bonfire.oss.bth4j;

/**
 * How much of a processor failure a runner records and reports, for hosts whose exception messages may carry
 * customer data. Set with {@link BackgroundRunner.Builder#failureDetail(FailureDetail)}.
 */
public enum FailureDetail {
    /** Class name, message and stack trace go into the stored failure, the events, the issues and the log. */
    FULL,
    /**
     * Only the exception class name, the attempt and the time are recorded. The stored failure has no message
     * and no stack, events carry a {@link today.bonfire.oss.bth4j.exceptions.RedactedFailure}, and the
     * runner's own log lines name the class only.
     */
    CLASS_NAME_ONLY
}
