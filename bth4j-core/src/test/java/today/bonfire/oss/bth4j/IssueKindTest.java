package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;

class IssueKindTest {

    @Test
    void everyKindHasTheTextTheAdminApiSends() {
        var expected = new LinkedHashMap<Issue.Kind, String>();
        expected.put(Issue.Kind.UNKNOWN_TYPE, "unknown-type");
        expected.put(Issue.Kind.STORE_ERROR, "store-error");
        expected.put(Issue.Kind.LEASE_LOST, "lease-lost");
        expected.put(Issue.Kind.RETRY_POLICY, "retry-policy");
        expected.put(Issue.Kind.EXECUTOR_REJECTED, "executor-rejected");
        expected.put(Issue.Kind.ATTEMPT_ABANDONED, "attempt-abandoned");
        expected.put(Issue.Kind.CORRUPT_TASK, "corrupt-task");
        expected.put(Issue.Kind.RESOURCE_SIGNAL_UNAVAILABLE, "resource-signal-unavailable");
        expected.put(Issue.Kind.SCHEDULE_MISFIRE, "schedule-misfire");
        expected.put(Issue.Kind.SCHEDULE_OVERLAP, "schedule-overlap");
        expected.put(Issue.Kind.SCHEDULE_PAUSED, "schedule-paused");
        expected.put(Issue.Kind.SCHEDULE_REARM, "schedule-rearm");
        expected.put(Issue.Kind.SCHEDULE_BROKEN, "schedule-broken");

        assertThat(Issue.Kind.values()).containsExactlyElementsOf(expected.keySet());
        expected.forEach((kind, text) -> assertThat(kind.text()).as(kind.name()).isEqualTo(text));
    }

    @Test
    void noTwoKindsShareAText() {
        assertThat(Arrays.stream(Issue.Kind.values()).map(Issue.Kind::text)).doesNotHaveDuplicates();
    }
}
