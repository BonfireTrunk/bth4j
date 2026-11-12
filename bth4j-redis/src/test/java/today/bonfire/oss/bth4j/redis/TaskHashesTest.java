package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static today.bonfire.oss.bth4j.redis.Keys.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.StepResult;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.RecordJson;
import today.bonfire.oss.bth4j.spi.TaskRecord;

class TaskHashesTest {

    private static final long AT = 1_800_000_000_000L;
    private static final long LEASE_UNTIL = AT + 30_000;
    private static final int RUNNER = 7;

    /** A queued task with every field set, including those an earlier attempt left behind. */
    private static TaskRecord everyField() {
        return TaskRecord.builder()
                .id(42)
                .revision(5)
                .queueId(3)
                .typeId(9)
                .payloadVersion(2)
                .state(TaskState.QUEUED)
                .attempts(1)
                .maxAttempts(4)
                .createdAt(Instant.ofEpochMilli(AT - 9_000))
                .dueAt(Instant.ofEpochMilli(AT - 1_000))
                .stateChangedAt(Instant.ofEpochMilli(AT - 1_000))
                .encodedPayload("{\"order\":1}")
                .runnerId(2)
                .leaseUntil(Instant.ofEpochMilli(AT - 2_000))
                .claimedAt(Instant.ofEpochMilli(AT - 5_000))
                .account("acct")
                .headers(Map.of("trace", "t-1"))
                .uniqueKey("key-1")
                .keyKeep(Duration.ofMinutes(5))
                .keyUntil(Instant.ofEpochMilli(AT + 60_000))
                .retryPolicy("{\"kind\":\"fixed\"}")
                .scheduleId("nightly")
                .retryOf(41L)
                .startBy(Instant.ofEpochMilli(AT + 3_600_000))
                .maxRunTime(Duration.ofMinutes(10))
                .cancelRequestedAt(Instant.ofEpochMilli(AT - 500))
                .cancelReason("operator")
                .progress("half")
                .steps(Map.of("charge", new StepResult(Instant.ofEpochMilli(AT - 4_000), "ok")))
                .failures(List.of(new FailureInfo(1, Instant.ofEpochMilli(AT - 3_000), "java.lang.Boom", "boom", null)))
                .build();
    }

    /** The task hash as the store writes it, with the fields the enqueue script sets from the store clock. */
    private static Map<String, String> hash(TaskRecord t) {
        var pairs = TaskHashes.fields(t);
        var h = new HashMap<String, String>();
        for (var i = 0; i < pairs.size(); i += 2) h.put(pairs.get(i), pairs.get(i + 1));
        h.put(F_STATE, String.valueOf(t.state().code()));
        h.put(F_DUE, String.valueOf(t.dueAt().toEpochMilli()));
        h.put(F_STATE_CHANGED, String.valueOf(t.stateChangedAt().toEpochMilli()));
        h.put("hs", t.steps() == null ? "0" : "1");
        h.put("fc", t.failures() == null ? "0" : "1");
        return h;
    }

    /** A CLAIM reply entry for the task, laid out as the script lays it out. */
    private static List<Object> entry(TaskRecord t, Map<String, String> h, String[] fields) {
        var e = new ArrayList<Object>();
        e.add(String.valueOf(t.id()));
        e.add(t.encodedPayload());
        e.add(t.steps() == null ? null : RecordJson.encodeSteps(t.steps()));
        e.add(
                t.failures() == null
                        ? List.of()
                        : t.failures().stream().map(RecordJson::encodeFailure).toList());
        for (var f : fields) e.add(h.get(f));
        return e;
    }

    private static List<Object> entry(TaskRecord t) {
        return entry(t, hash(t), RedisScripts.CLAIM_FIELDS);
    }

    private static TaskRecord leased(TaskRecord t) {
        return t.toBuilder()
                .state(TaskState.LEASED)
                .revision(t.revision() + 1)
                .attempts(t.attempts() + 1)
                .stateChangedAt(Instant.ofEpochMilli(AT))
                .claimedAt(Instant.ofEpochMilli(AT))
                .leaseUntil(Instant.ofEpochMilli(LEASE_UNTIL))
                .runnerId(RUNNER)
                .build();
    }

    @Test
    void aClaimEntryDecodesToTheLeasedTaskTheHashReadAgrees() {
        var t = everyField();

        var claimed = TaskHashes.fromClaim(entry(t), AT, LEASE_UNTIL, RUNNER);

        assertThat(claimed).isEqualTo(leased(t));
        var h = hash(t);
        h.put(F_STATE, String.valueOf(TaskState.LEASED.code()));
        h.put(F_STATE_CHANGED, String.valueOf(AT));
        h.put(F_REVISION, String.valueOf(t.revision() + 1));
        h.put(F_ATTEMPTS, String.valueOf(t.attempts() + 1));
        h.put(F_CLAIMED_AT, String.valueOf(AT));
        h.put(F_LEASE, String.valueOf(LEASE_UNTIL));
        h.put(F_RUNNER, String.valueOf(RUNNER));
        assertThat(TaskHashes.fromHash(
                        t.id(),
                        h,
                        t.encodedPayload(),
                        RecordJson.encodeSteps(t.steps()),
                        t.failures().stream().map(RecordJson::encodeFailure).toList()))
                .isEqualTo(claimed);
    }

    @Test
    void aClaimEntryWithOnlyTheRequiredFieldsLeavesTheOthersEmpty() {
        var t = TaskRecord.builder()
                .id(8)
                .revision(1)
                .queueId(1)
                .typeId(1)
                .state(TaskState.QUEUED)
                .maxAttempts(3)
                .createdAt(Instant.ofEpochMilli(AT - 9_000))
                .dueAt(Instant.ofEpochMilli(AT - 1_000))
                .stateChangedAt(Instant.ofEpochMilli(AT - 1_000))
                .build();

        var claimed = TaskHashes.fromClaim(entry(t), AT, LEASE_UNTIL, RUNNER);

        assertThat(claimed).isEqualTo(leased(t));
        assertThat(claimed.encodedPayload()).isNull();
        assertThat(claimed.headers()).isNull();
        assertThat(claimed.steps()).isNull();
        assertThat(claimed.failures()).isNull();
        assertThat(claimed.retryOf()).isNull();
        assertThat(claimed.keyKeep()).isNull();
    }

    @Test
    void aClaimEntryOfAnotherShapeIsAStoreErrorThatDoesNotQuoteItsValues() {
        var full = entry(everyField());
        var shorter = full.subList(0, full.size() - 1);

        assertThatThrownBy(() -> TaskHashes.fromClaim(shorter, AT, LEASE_UNTIL, RUNNER))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining(full.size() - 1 + " values")
                .hasMessageNotContaining("order");
        assertThatThrownBy(() -> TaskHashes.fromClaim("secret", AT, LEASE_UNTIL, RUNNER))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("String")
                .hasMessageNotContaining("secret");
        assertThatThrownBy(() -> TaskHashes.fromClaim(null, AT, LEASE_UNTIL, RUNNER))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("null");
    }

    @Test
    void aClaimValueThatIsNotTextNamesTheTaskAndTheField() {
        var e = entry(everyField());
        e.set(4 + Arrays.asList(RedisScripts.CLAIM_FIELDS).indexOf(F_QUEUE), 3L);

        assertThatThrownBy(() -> TaskHashes.fromClaim(e, AT, LEASE_UNTIL, RUNNER))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("task 42")
                .hasMessageContaining("Long as " + F_QUEUE);
    }

    @Test
    void aMalformedClaimIdIsAStoreError() {
        var e = entry(everyField());
        e.set(0, "4x2");

        assertThatThrownBy(() -> TaskHashes.fromClaim(e, AT, LEASE_UNTIL, RUNNER))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("malformed task id 4x2");
    }

    @Test
    void aStoredValueThatCannotBeReadIsAnUnreadableTaskNamingTheTaskOnBothPaths() {
        var t = everyField();
        for (var bad : List.of(
                Map.entry(F_REVISION, "five"),
                Map.entry(F_STATE, "9"),
                Map.entry(F_DUE, "soon"),
                Map.entry(F_HEADERS, "{not json"),
                Map.entry(F_RETRY_OF, "41a"))) {
            var h = hash(t);
            h.put(bad.getKey(), bad.getValue());

            assertThatThrownBy(
                            () -> TaskHashes.fromClaim(entry(t, h, RedisScripts.CLAIM_FIELDS), AT, LEASE_UNTIL, RUNNER))
                    .as(bad.getKey())
                    .isInstanceOf(UnreadableTaskException.class)
                    .hasMessageContaining("task 42");
            assertThatThrownBy(() -> TaskHashes.fromHash(t.id(), h, null, null, List.of()))
                    .as(bad.getKey())
                    .isInstanceOf(UnreadableTaskException.class)
                    .hasMessageContaining("task 42");
        }
        assertThatThrownBy(() -> TaskHashes.fromHash(t.id(), hash(t), null, null, List.of("[1]")))
                .isInstanceOf(UnreadableTaskException.class)
                .hasMessageContaining("task 42");
    }

    @Test
    void aFieldTheRecordNeedsButTheClaimReplyDoesNotCarryFailsLoudly() {
        var t = everyField();
        var withoutAccount = Arrays.stream(RedisScripts.CLAIM_FIELDS)
                .filter(f -> !f.equals(F_ACCOUNT))
                .toArray(String[]::new);

        assertThatThrownBy(() -> TaskHashes.fromClaim(
                        entry(t, hash(t), withoutAccount), withoutAccount, AT, LEASE_UNTIL, RUNNER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not carry the task field " + F_ACCOUNT);
    }

    @Test
    void reorderedClaimFieldsDecodeTheSameTask() {
        var t = everyField();
        var reversed = Arrays.asList(RedisScripts.CLAIM_FIELDS.clone());
        Collections.reverse(reversed);
        var fields = reversed.toArray(String[]::new);

        assertThat(TaskHashes.fromClaim(entry(t, hash(t), fields), fields, AT, LEASE_UNTIL, RUNNER))
                .isEqualTo(leased(t));
    }

    @Test
    void aFieldListedTwiceIsRefused() {
        assertThatThrownBy(() -> TaskHashes.positions(new String[] {"rev", "q", "rev"}, 4))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("rev is listed twice");
        assertThat(TaskHashes.positions(RedisScripts.CLAIM_FIELDS, 4)).hasSize(RedisScripts.CLAIM_FIELDS.length);
    }

    @Test
    void theClaimScriptHasEveryFieldPositionFilledIn() {
        assertThat(RedisScripts.CLAIM).doesNotContainPattern("@[A-Za-z]+@");
        var state = Arrays.asList(RedisScripts.CLAIM_FIELDS).indexOf(F_STATE) + 1;
        assertThat(RedisScripts.CLAIM).contains("if v[" + state + "] ~= '0'");
    }
}
