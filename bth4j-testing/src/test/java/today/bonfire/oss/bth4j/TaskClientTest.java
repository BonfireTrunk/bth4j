package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.EnqueueFailedException;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.exceptions.TaskIdReusedException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
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
    void reusedIdWithTheSameContentIsAcceptedAgain() {
        var client = builder().build();
        long id = client.nextId();
        var request = TaskRequest.of(TYPED, new Payload("same"))
                .queue("Q:LOW")
                .uniqueKey("k")
                .id(id)
                .build();

        client.enqueue(request);

        assertThat(client.enqueue(request)).isEqualTo(new EnqueueResult.Accepted(id));
    }

    @Test
    void reusedIdWithAnotherPayloadTypeQueueOrKeyThrows() {
        var client = builder().build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(TYPED, new Payload("a")).id(id).build());

        assertThatThrownBy(() -> client.enqueue(
                        TaskRequest.of(TYPED, new Payload("b")).id(id).build()))
                .isInstanceOfSatisfying(
                        TaskIdReusedException.class, e -> assertThat(e.id()).isEqualTo(id));
        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).id(id).build()))
                .isInstanceOf(TaskIdReusedException.class);
        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(TYPED, new Payload("a"))
                        .queue("Q:LOW")
                        .id(id)
                        .build()))
                .isInstanceOf(TaskIdReusedException.class);
        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(TYPED, new Payload("a"))
                        .uniqueKey("k")
                        .id(id)
                        .build()))
                .isInstanceOf(TaskIdReusedException.class);
        assertThat(client.tasks(TaskState.QUEUED, null, 0, 100)).hasSize(1);
    }

    @Test
    void anIdAllocatedLaterToAnotherTaskIsNotAnsweredAccepted() {
        var client = builder().idBlockSize(1).build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(TYPED, new Payload("first")).id(id).build());

        assertThatThrownBy(() -> client.enqueue(
                        TaskRequest.of(TYPED, new Payload("second")).id(id).build()))
                .isInstanceOf(TaskIdReusedException.class);
    }

    @Test
    void enqueueAllChecksReusedIds() {
        var client = builder().build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(TYPED, new Payload("a")).id(id).build());

        var same = client.enqueueAll(List.of(
                TaskRequest.of(TYPED, new Payload("a")).id(id).build(),
                TaskRequest.of(VOID).build()));
        assertThat(same.getFirst()).isEqualTo(new EnqueueResult.Accepted(id));
        assertThatThrownBy(() ->
                        client.enqueueAll(List.of(TaskRequest.of(VOID).id(id).build())))
                .isInstanceOf(TaskIdReusedException.class);
    }

    @Test
    void anIdThatWasNeverAllocatedIsRejectedAndNothingIsStored() {
        var client = builder().build();
        long next = client.nextId();
        long unallocated = store.idHighWaterMark();

        assertThatThrownBy(() ->
                        client.enqueue(TaskRequest.of(VOID).id(unallocated).build()))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("never allocated");
        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(),
                        TaskRequest.of(VOID).id(unallocated + 5).build())))
                .isInstanceOf(TaskValidationException.class);
        assertThat(client.enqueue(TaskRequest.of(VOID).id(next).build()).accepted())
                .isTrue();
        assertThat(client.tasks(TaskState.QUEUED, null, 0, 100)).hasSize(1);
    }

    private FullStore failingStore(AtomicBoolean down) {
        return (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, method, args) -> {
                    if (down.get() && !method.getName().equals("init")) {
                        throw new TaskStoreException("store down");
                    }
                    try {
                        return method.invoke(store, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private FullStore storeFailingOn(String... methods) {
        var names = java.util.Set.of(methods);
        return (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, method, args) -> {
                    if (names.contains(method.getName())) throw new TaskStoreException("store down");
                    try {
                        return method.invoke(store, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private TaskClient clientOn(FullStore s) {
        return TaskClient.builder()
                .store(s)
                .jsonMapper(new JsonMapperTest())
                .clock(clock)
                .build();
    }

    @Test
    void aGeneratedIdThatIsTakenIsNeverAnsweredAcceptedAndTheTaskIsStoredUnderANewId() {
        var b = builder().idBlockSize(10).build();
        var a = builder().build();
        assertThat(b.nextId()).isEqualTo(1);
        a.enqueue(TaskRequest.of(VOID).id(2).build());

        var result = b.enqueue(TaskRequest.of(VOID).build());

        assertThat(result.accepted()).isTrue();
        assertThat(result.id()).isEqualTo(3);
        assertThat(b.tasks(TaskState.QUEUED, null, 0, 100))
                .extracting(TaskView::id)
                .containsExactly(2L, 3L);
    }

    @Test
    void aTakenGeneratedIdWithOtherContentDoesNotThrowAtTheInnocentClient() {
        var b = builder().idBlockSize(10).build();
        var a = builder().build();
        b.nextId();
        a.enqueue(TaskRequest.of(TYPED, new Payload("a")).id(2).build());

        assertThat(b.enqueue(TaskRequest.of(VOID).build()).id()).isEqualTo(3);
    }

    @Test
    void enqueueAllReplacesTakenGeneratedIdsAndKeepsTheOrder() {
        var b = builder().idBlockSize(10).build();
        var a = builder().build();
        b.nextId();
        a.enqueue(TaskRequest.of(VOID).id(3).build());

        var results = b.enqueueAll(List.of(
                TaskRequest.of(VOID).build(),
                TaskRequest.of(VOID).build(),
                TaskRequest.of(VOID).build()));

        assertThat(results)
                .extracting(EnqueueResult::id)
                .hasSize(3)
                .doesNotHaveDuplicates()
                .doesNotContain(1L);
        assertThat(results.get(0).id()).isEqualTo(2);
        assertThat(results.get(2).id()).isEqualTo(4);
        assertThat(results.get(1).id()).isGreaterThan(4);
        assertThat(b.tasks(TaskState.QUEUED, null, 0, 100)).hasSize(4);
    }

    @Test
    void enqueueAllChecksEveryCallerIdBeforeWritingAnything() {
        var client = builder().build();
        long stored = client.nextId();
        long other = client.nextId();
        client.enqueue(TaskRequest.of(VOID).id(other).build());

        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(),
                        TaskRequest.of(TYPED, new Payload("x")).id(other).build(),
                        TaskRequest.of(VOID).id(stored).build())))
                .isInstanceOf(TaskIdReusedException.class);

        assertThat(client.tasks(TaskState.QUEUED, null, 0, 100))
                .extracting(TaskView::id)
                .containsExactly(other);
    }

    @Test
    void enqueueAllRejectsOneCallerIdOnTwoRequests() {
        var client = builder().build();
        long id = client.nextId();

        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).id(id).build(),
                        TaskRequest.of(VOID).id(id).build())))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("more than one");
        assertThat(client.tasks(TaskState.QUEUED, null, 0, 100)).isEmpty();
    }

    @Test
    void failedMarkReadInEnqueueAllCarriesEveryCallerId() {
        var client = clientOn(storeFailingOn("idHighWaterMark"));
        long first = client.nextId();
        long second = client.nextId();

        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).id(first).build(),
                        TaskRequest.of(VOID).id(second).build())))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).containsExactly(first, second));
    }

    @Test
    void failedMarkReadInAMixedBatchCarriesTheFullListWithTheGeneratedIds() {
        var client = clientOn(storeFailingOn("idHighWaterMark"));
        long mine = client.nextId();

        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(),
                        TaskRequest.of(VOID).id(mine).build())))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).hasSize(2);
                    assertThat(e.ids().get(1)).isEqualTo(mine);
                    assertThat(e.ids().get(0)).isNotEqualTo(mine);
                });
    }

    @Test
    void coldNamesWithTheStoreDownCarryTheCallerIdInBothCalls() {
        var client = clientOn(storeFailingOn("queueId", "eventId"));
        long id = client.nextId();

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).id(id).build()))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).containsExactly(id));
        assertThatThrownBy(() ->
                        client.enqueueAll(List.of(TaskRequest.of(VOID).id(id).build())))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).containsExactly(id));
    }

    @Test
    void aFinishedTaskThatReleasedItsKeyAcceptsARetryWithAKey() {
        var client = builder().build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(VOID).id(id).build());
        var task = client.task(id).orElseThrow();
        client.admin().cancel(id, task.revision(), "done");
        assertThat(client.task(id).orElseThrow().state().isTerminal()).isTrue();

        assertThat(client.enqueue(TaskRequest.of(VOID).uniqueKey("k").id(id).build()))
                .isEqualTo(new EnqueueResult.Accepted(id));
    }

    @Test
    void aQueuedTaskWithoutAKeyRejectsARetryWithAKey() {
        var client = builder().build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(VOID).id(id).build());

        assertThatThrownBy(() -> client.enqueue(
                        TaskRequest.of(VOID).uniqueKey("k").id(id).build()))
                .isInstanceOf(TaskIdReusedException.class);
    }

    @Test
    void aHeldKeyRejectsARetryWithoutIt() {
        var client = builder().build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(VOID).uniqueKey("k").id(id).build());

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).id(id).build()))
                .isInstanceOf(TaskIdReusedException.class);
    }

    @Test
    void storeDownOnAColdClientThrowsEnqueueFailedWithoutIds() {
        var down = new AtomicBoolean();
        var client = TaskClient.builder()
                .store(failingStore(down))
                .jsonMapper(new JsonMapperTest())
                .clock(clock)
                .build();
        down.set(true);

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).build()))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).isEmpty());
        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(), TaskRequest.of(VOID).build())))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).isEmpty());
    }

    @Test
    void storeDownWhileCheckingACallerIdCarriesThatId() {
        var down = new AtomicBoolean();
        var client = TaskClient.builder()
                .store(failingStore(down))
                .jsonMapper(new JsonMapperTest())
                .clock(clock)
                .build();
        long id = client.nextId();
        down.set(true);

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).id(id).build()))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).containsExactly(id));
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
    void tasksWalksWithACursor() {
        var client = builder().build();
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 5; i++)
            ids.add(client.enqueue(TaskRequest.of(VOID).build()).id());

        var first = client.tasks(TaskState.QUEUED, "Q:DEF", (String) null, 2);
        var second = client.tasks(TaskState.QUEUED, "Q:DEF", first.next(), 2);
        var third = client.tasks(TaskState.QUEUED, "Q:DEF", second.next(), 2);

        assertThat(first.items()).extracting(TaskView::id).containsExactlyElementsOf(ids.subList(0, 2));
        assertThat(second.items()).extracting(TaskView::id).containsExactlyElementsOf(ids.subList(2, 4));
        assertThat(third.items()).extracting(TaskView::id).containsExactlyElementsOf(ids.subList(4, 5));
        assertThat(third.next()).isNull();
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
