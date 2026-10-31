package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

class TaskClientTest {

    public record Payload(String text) {}

    private static final TaskType<Payload> TYPED = TaskType.of("client-typed", Payload.class);
    private static final TaskType<Void> VOID = TaskType.of("client-void");

    private MutableClock clock;
    private InMemoryStore store;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        store = new InMemoryStore(clock);
    }

    private TaskClient.Builder builder() {
        return TaskClient.builder()
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .clock(clock);
    }

    @Test
    void enqueueAcceptsAndIdsIncrease() {
        var client = builder().build();

        var first = client.enqueue(TaskRequest.of(VOID).build());
        var second = client.enqueue(TaskRequest.of(VOID).build());
        var third = client.enqueue(TaskRequest.of(TYPED, new Payload("x")).build());

        assertThat(first).isInstanceOf(EnqueueResult.Accepted.class);
        assertThat(first.accepted()).isTrue();
        assertThat(second.id()).isGreaterThan(first.id());
        assertThat(third.id()).isGreaterThan(second.id());
    }

    @Test
    void idsAreReservedInBlocksOfTheConfiguredSize() {
        var client = builder().idBlockSize(10).build();

        client.enqueue(TaskRequest.of(VOID).build());
        long afterOne = store.allocateIds(1);
        for (int i = 0; i < 9; i++) client.enqueue(TaskRequest.of(VOID).build());

        assertThat(afterOne).isEqualTo(11L);
        assertThat(store.allocateIds(1)).isEqualTo(12L);

        client.enqueue(TaskRequest.of(VOID).build());

        assertThat(store.allocateIds(1)).isEqualTo(23L);
    }

    @Test
    void blockSizeOneReservesPerEnqueue() {
        var client = builder().idBlockSize(1).build();

        var first = client.enqueue(TaskRequest.of(VOID).build());
        long gap = store.allocateIds(1);
        var second = client.enqueue(TaskRequest.of(VOID).build());

        assertThat(gap).isEqualTo(first.id() + 1);
        assertThat(second.id()).isEqualTo(gap + 1);
    }

    @Test
    void sameRequestIdIsAcceptedTwiceAndStoredOnce() {
        var client = builder().build();
        long id = client.nextId();
        var request = TaskRequest.of(TYPED, new Payload("once")).id(id).build();

        var first = client.enqueue(request);
        var second = client.enqueue(request);

        assertThat(first).isEqualTo(new EnqueueResult.Accepted(id));
        assertThat(second).isEqualTo(new EnqueueResult.Accepted(id));
        assertThat(client.tasks(TaskState.QUEUED, null, 0, 100))
                .extracting(TaskView::id)
                .containsExactly(id);
    }

    @Test
    void uniqueKeyAnswersDuplicateWithTheHolder() {
        var client = builder().build();

        var first = client.enqueue(TaskRequest.of(VOID).uniqueKey("k").build());
        var second = client.enqueue(TaskRequest.of(VOID).uniqueKey("k").build());
        var other = client.enqueue(TaskRequest.of(VOID).uniqueKey("other").build());

        assertThat(first.accepted()).isTrue();
        assertThat(second).isEqualTo(new EnqueueResult.Duplicate(first.id()));
        assertThat(second.accepted()).isFalse();
        assertThat(second.id()).isEqualTo(first.id());
        assertThat(other.accepted()).isTrue();
        assertThat(client.tasks(TaskState.QUEUED, null, 0, 100)).hasSize(2);
    }

    @Test
    void enqueueAllAnswersInOrderAndMixesAcceptedWithDuplicate() {
        var client = builder().build();
        var requests = List.of(
                TaskRequest.of(VOID).uniqueKey("a").build(),
                TaskRequest.of(VOID).uniqueKey("a").build(),
                TaskRequest.of(VOID).build());

        var results = client.enqueueAll(requests);

        assertThat(results).hasSize(3);
        assertThat(results.get(0)).isInstanceOf(EnqueueResult.Accepted.class);
        assertThat(results.get(1))
                .isEqualTo(new EnqueueResult.Duplicate(results.get(0).id()));
        assertThat(results.get(2)).isInstanceOf(EnqueueResult.Accepted.class);
    }

    @Test
    void enqueueAllHandlesMoreThanOneChunk() {
        var client = builder().build();
        var requests = new ArrayList<TaskRequest<Void>>();
        for (int i = 0; i < 1200; i++) requests.add(TaskRequest.of(VOID).build());

        var results = client.enqueueAll(requests);

        assertThat(results).hasSize(1200).allMatch(EnqueueResult::accepted);
        assertThat(results.stream().map(EnqueueResult::id).distinct().count()).isEqualTo(1200L);
        assertThat(client.counts().get("Q:DEF").queued()).isEqualTo(1200L);
    }

    @Test
    void headersAtTheLimitAreStoredAndOverTheLimitAreRejected() {
        var client = builder().build();
        var fits = "x".repeat(TaskRequest.MAX_HEADERS_BYTES - 1);

        var accepted = client.enqueue(TaskRequest.of(VOID).header("h", fits).build());

        assertThat(client.task(accepted.id()).orElseThrow().headers()).containsEntry("h", fits);
        assertThatThrownBy(() -> client.enqueue(
                        TaskRequest.of(VOID).header("h", fits + "x").build()))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("headers");
    }

    @Test
    void headerLimitCountsUtf8BytesOfNamesAndValues() {
        var client = builder().build();
        var twoBytes = "é".repeat(TaskRequest.MAX_HEADERS_BYTES / 4);
        var request = TaskRequest.of(VOID)
                .header("a", twoBytes)
                .header("b", twoBytes)
                .header("c", "x")
                .build();

        assertThatThrownBy(() -> client.enqueue(request)).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void payloadOverTheLimitIsRejectedAndNothingIsStored() {
        var client = builder().build();
        var tooBig = new Payload("x".repeat(TaskRequest.MAX_PAYLOAD_BYTES));

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(TYPED, tooBig).build()))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("exceeds");
        assertThat(client.tasks(TaskState.QUEUED, null, 0, 10)).isEmpty();
    }

    @Test
    void payloadBelowTheLimitIsStoredEncoded() {
        var client = builder().build();
        var big = new Payload("x".repeat(TaskRequest.MAX_PAYLOAD_BYTES - 100));

        var result = client.enqueue(TaskRequest.of(TYPED, big).build());

        assertThat(client.task(result.id()).orElseThrow().data()).hasSize(TaskRequest.MAX_PAYLOAD_BYTES - 100 + 11);
    }

    @Test
    void taskViewResolvesQueueAndEventNames() {
        var client = builder().build();

        var result = client.enqueue(TaskRequest.of(TYPED, new Payload("v"))
                .queue("Q:HIGH")
                .account("acme")
                .maxAttempts(4)
                .build());

        var view = client.task(result.id()).orElseThrow();
        assertThat(view.id()).isEqualTo(result.id());
        assertThat(view.queue()).isEqualTo("Q:HIGH");
        assertThat(view.event()).isEqualTo("client-typed");
        assertThat(view.state()).isEqualTo(TaskState.QUEUED);
        assertThat(view.account()).isEqualTo("acme");
        assertThat(view.maxAttempts()).isEqualTo(4);
        assertThat(view.attempts()).isZero();
        assertThat(view.payloadVersion()).isEqualTo(1);
        assertThat(view.data()).contains("\"text\":\"v\"");
    }

    @Test
    void requestsWithoutQueueUseTheDefaultQueueAndDefaultMaxAttempts() {
        var client = builder().defaultMaxAttempts(7).build();

        var view =
                client.task(client.enqueue(TaskRequest.of(VOID).build()).id()).orElseThrow();

        assertThat(view.queue()).isEqualTo("Q:DEF");
        assertThat(view.maxAttempts()).isEqualTo(7);
    }

    @Test
    void unknownTaskIsEmptyAndUnknownQueueIsRejected() {
        var client = builder().build();

        assertThat(client.task(999_999)).isEmpty();
        assertThatThrownBy(() ->
                        client.enqueue(TaskRequest.of(VOID).queue("Q:NOPE").build()))
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void customQueuesReplaceTheDefaults() {
        var client = builder().queues(List.of("A", "B"), "B").build();

        var defaulted =
                client.task(client.enqueue(TaskRequest.of(VOID).build()).id()).orElseThrow();

        assertThat(defaulted.queue()).isEqualTo("B");
        assertThatThrownBy(
                        () -> client.enqueue(TaskRequest.of(VOID).queue("Q:DEF").build()))
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void delayedTaskIsScheduledAtTheClientClockPlusDelay() {
        var client = builder().build();

        var result =
                client.enqueue(TaskRequest.of(VOID).delay(Duration.ofMinutes(5)).build());

        var view = client.task(result.id()).orElseThrow();
        assertThat(view.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(view.due()).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
    }

    @Test
    void startByBeforeTheRunTimeIsRejected() {
        var client = builder().build();
        var request = TaskRequest.of(VOID)
                .delay(Duration.ofMinutes(5))
                .startBy(clock.instant().plusSeconds(10))
                .build();

        assertThatThrownBy(() -> client.enqueue(request)).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void countsAreGroupedByQueueName() {
        var client = builder().build();
        client.enqueue(TaskRequest.of(VOID).build());
        client.enqueue(TaskRequest.of(VOID).build());
        client.enqueue(TaskRequest.of(VOID).queue("Q:HIGH").build());
        client.enqueue(
                TaskRequest.of(VOID).queue("Q:HIGH").delay(Duration.ofHours(1)).build());

        var counts = client.counts();

        assertThat(counts).containsOnlyKeys("Q:DEF", "Q:HIGH");
        assertThat(counts.get("Q:DEF").queued()).isEqualTo(2L);
        assertThat(counts.get("Q:HIGH").queued()).isEqualTo(1L);
        assertThat(counts.get("Q:HIGH").scheduled()).isEqualTo(1L);
        assertThat(counts.get("Q:HIGH").of(TaskState.DEAD)).isZero();
    }

    @Test
    void tasksListsOneStateOfOneQueueWithPaging() {
        var client = builder().build();
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 5; i++)
            ids.add(client.enqueue(TaskRequest.of(VOID).build()).id());
        client.enqueue(TaskRequest.of(VOID).queue("Q:LOW").build());

        var all = client.tasks(TaskState.QUEUED, "Q:DEF", 0, 100);
        var firstPage = client.tasks(TaskState.QUEUED, "Q:DEF", 0, 2);
        var secondPage =
                client.tasks(TaskState.QUEUED, "Q:DEF", firstPage.getLast().id(), 100);

        assertThat(all).extracting(TaskView::id).containsExactlyElementsOf(ids);
        assertThat(firstPage).extracting(TaskView::id).containsExactlyElementsOf(ids.subList(0, 2));
        assertThat(secondPage).extracting(TaskView::id).containsExactlyElementsOf(ids.subList(2, 5));
        assertThat(client.tasks(TaskState.SUCCEEDED, null, 0, 100)).isEmpty();
    }

    @Test
    void schedulePutDeleteAndList() {
        var client = builder().build();
        var definition = ScheduleDefinition.of("nightly", TYPED, new Payload("p"), "0 3 * * *");

        var next = client.putSchedule(definition);

        assertThat(next).isEqualTo(Instant.parse("2030-01-01T03:00:00Z"));
        assertThat(client.schedules()).containsEntry("nightly", next);

        assertThat(client.deleteSchedule("nightly")).isTrue();
        assertThat(client.deleteSchedule("nightly")).isFalse();
        assertThat(client.schedules()).isEmpty();
    }

    @Test
    void registeringAScheduleAgainKeepsTheNextFireTimeForTheSameCronAndResetsItForAnother() {
        var client = builder().build();
        var first = client.putSchedule(ScheduleDefinition.of("s", VOID, "0 3 * * *"));
        clock.advance(Duration.ofHours(1));

        var same = client.putSchedule(ScheduleDefinition.of("s", VOID, "0 3 * * *"));
        var changed = client.putSchedule(ScheduleDefinition.of("s", VOID, "0 5 * * *"));

        assertThat(first).isEqualTo(Instant.parse("2030-01-01T03:00:00Z"));
        assertThat(same).isEqualTo(first);
        assertThat(changed).isEqualTo(Instant.parse("2030-01-01T05:00:00Z"));
        assertThat(client.schedules()).containsOnly(java.util.Map.entry("s", changed));
    }

    @Test
    void invalidCronIsRejected() {
        var client = builder().build();

        assertThatThrownBy(() -> client.putSchedule(ScheduleDefinition.of("bad", VOID, "not a cron")))
                .isInstanceOf(TaskValidationException.class);
        assertThat(client.schedules()).isEmpty();
    }

    @Test
    void scheduleDefinitionValidatesItsFields() {
        assertThatThrownBy(() -> ScheduleDefinition.of(" ", VOID, "* * * * *"))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.of("x".repeat(129), VOID, "* * * * *"))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.of("id", TYPED, null, "* * * * *"))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.of("id", VOID, " ")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.of("id", VOID, "* * * * *").maxAttempts(0))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void builderRequiresAStoreAndValidatesItsSettings() {
        assertThatThrownBy(() -> TaskClient.builder().build()).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> TaskClient.builder().idBlockSize(0)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> TaskClient.builder().defaultMaxAttempts(0)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> TaskClient.builder().clock(null)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> TaskClient.builder().queues(List.of(), null))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> TaskClient.builder().queues(List.of("A"), "B"))
                .isInstanceOf(TaskConfigurationError.class);
    }
}
