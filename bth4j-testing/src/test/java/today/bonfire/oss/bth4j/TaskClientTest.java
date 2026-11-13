package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.EnqueueFailedException;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.TaskIdReusedException;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.TaskRecord;
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
        store = InMemoryStore.builder().clock(clock).build();
    }

    private static final Queues QUEUES = Queues.of("Q:DEF", "Q:LOW", "Q:MED", "Q:HIGH");

    private TaskClient.Builder builder() {
        return TaskClient.builder()
                .queues(QUEUES)
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .clock(clock);
    }

    @Test
    void aMissingArgumentIsRefusedAsAValidationError() {
        var client = builder().build();

        assertThatThrownBy(() -> client.enqueue(null))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("request is required");
        assertThatThrownBy(() -> client.enqueueAll(null))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("requests are required");
        assertThatThrownBy(() -> client.enqueueAll(
                        java.util.Arrays.asList(TaskRequest.of(VOID).build(), null)))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("requests cannot contain null");
        assertThatThrownBy(() -> client.putSchedule(null))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("definition is required");
        assertThatThrownBy(() -> client.tasks(null, null, null, 10))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("state is required");
        assertThat(store.counts().values()).allMatch(StateCounts.ZERO::equals);
    }

    @Test
    void aTaskListingRefusesABlankQueueAndListsEveryQueueForNull() {
        var client = builder().build();
        client.enqueue(TaskRequest.of(VOID).queue("Q:LOW").build());

        assertThatThrownBy(() -> client.tasks(TaskState.QUEUED, " ", null, 10))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("queue is required");
        assertThat(client.tasks(TaskState.QUEUED, null, null, 10).items()).hasSize(1);
        assertThat(client.tasks(TaskState.QUEUED, "Q:DEF", null, 10).items()).isEmpty();
    }

    @Test
    void enqueueAcceptsAndIdsIncrease() {
        var client = builder().build();

        var first = client.enqueue(TaskRequest.of(VOID).build());
        var second = client.enqueue(TaskRequest.of(VOID).build());
        var third = client.enqueue(TaskRequest.of(TYPED, new Payload("x")).build());

        assertThat(first).isInstanceOf(EnqueueResult.Accepted.class);
        assertThat(first.isAccepted()).isTrue();
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
        assertThat(client.tasks(TaskState.QUEUED, null, null, 100).items())
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
    void reusedIdWithAnotherPayloadTypeOrQueueThrows() {
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
        assertThat(client.tasks(TaskState.QUEUED, null, null, 100).items()).hasSize(1);
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
        assertThat(client.enqueue(TaskRequest.of(VOID).id(next).build()).isAccepted())
                .isTrue();
        assertThat(client.tasks(TaskState.QUEUED, null, null, 100).items()).hasSize(1);
    }

    private FullStore failingStore(AtomicBoolean down) {
        return interceptStore((method, args) -> {
            if (down.get() && !method.equals("init")) throw new TaskStoreException("store down");
        });
    }

    private FullStore storeFailingOn(String... methods) {
        var names = java.util.Set.of(methods);
        return interceptStore((method, args) -> {
            if (names.contains(method)) throw new TaskStoreException("store down");
        });
    }

    private FullStore interceptStore(BiConsumer<String, Object[]> before) {
        return (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, method, args) -> {
                    before.accept(method.getName(), args);
                    try {
                        return method.invoke(store, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private TaskClient clientOn(FullStore s) {
        return TaskClient.builder()
                .queues(QUEUES)
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

        assertThat(result.isAccepted()).isTrue();
        assertThat(result.id()).isEqualTo(11);
        assertThat(b.tasks(TaskState.QUEUED, null, null, 100).items())
                .extracting(TaskView::id)
                .containsExactly(2L, 11L);
    }

    @Test
    void aTakenGeneratedIdWithOtherContentDoesNotThrowAtTheInnocentClient() {
        var b = builder().idBlockSize(10).build();
        var a = builder().build();
        b.nextId();
        a.enqueue(TaskRequest.of(TYPED, new Payload("a")).id(2).build());

        assertThat(b.enqueue(TaskRequest.of(VOID).build()).id()).isEqualTo(11);
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
        assertThat(b.tasks(TaskState.QUEUED, null, null, 100).items()).hasSize(4);
    }

    @Test
    void enqueueAllChecksEveryCallerIdBeforeWritingAnything() {
        var client = builder().build();
        long stored = client.nextId();
        long other = client.nextId();
        client.enqueue(TaskRequest.of(VOID).id(other).build());

        var requests = new ArrayList<TaskRequest<?>>();
        for (int i = 0; i < 501; i++) requests.add(TaskRequest.of(VOID).build());
        requests.add(TaskRequest.of(TYPED, new Payload("x")).id(other).build());
        requests.add(TaskRequest.of(VOID).id(stored).build());

        assertThatThrownBy(() -> client.enqueueAll(requests)).isInstanceOf(TaskIdReusedException.class);

        assertThat(client.tasks(TaskState.QUEUED, null, null, 100).items())
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
        assertThat(client.tasks(TaskState.QUEUED, null, null, 100).items()).isEmpty();
    }

    @Test
    void failedMarkReadInEnqueueAllHasNoIds() {
        var client = clientOn(storeFailingOn("idHighWaterMark"));
        long first = store.allocateIds(1);
        long second = store.allocateIds(1);

        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).id(first).build(),
                        TaskRequest.of(VOID).id(second).build())))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).isEmpty());
    }

    @Test
    void failedMarkReadInAMixedBatchDoesNotReserveIds() {
        var allocations = new AtomicInteger();
        var client = clientOn(interceptStore((method, args) -> {
            if (method.equals("allocateIds")) allocations.incrementAndGet();
            if (method.equals("idHighWaterMark")) throw new TaskStoreException("store down");
        }));
        long mine = client.nextId();
        long external = store.allocateIds(1);

        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(),
                        TaskRequest.of(VOID).id(external).build())))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).isEmpty();
                    assertThat(e.storedCount()).isZero();
                });
        assertThat(allocations.get()).isEqualTo(1);
        assertThat(client.nextId()).isEqualTo(mine + 1);
    }

    @Test
    void coldNamesWithTheStoreDownCarryTheCallerIdInBothCalls() {
        var client = clientOn(storeFailingOn("queueId", "typeId"));
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
        client.enqueue(TaskRequest.of(VOID).uniqueKey("k").id(id).build());
        var task = client.task(id).orElseThrow();
        client.admin().cancel(id, task.revision(), "done");
        assertThat(client.task(id).orElseThrow().state().isTerminal()).isTrue();
        assertThat(client.task(id).orElseThrow().uniqueKey()).isNull();

        assertThat(client.enqueue(TaskRequest.of(VOID).uniqueKey("k").id(id).build()))
                .isEqualTo(new EnqueueResult.Accepted(id));
        assertThat(client.enqueueAll(
                        List.of(TaskRequest.of(VOID).uniqueKey("k").id(id).build())))
                .containsExactly(new EnqueueResult.Accepted(id));
    }

    @Test
    void aQueuedTaskWithoutAKeyAcceptsARetryWithAKey() {
        var client = builder().build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(VOID).id(id).build());

        assertThat(client.enqueue(TaskRequest.of(VOID).uniqueKey("k").id(id).build()))
                .isEqualTo(new EnqueueResult.Accepted(id));
        assertThat(client.enqueueAll(
                        List.of(TaskRequest.of(VOID).uniqueKey("k").id(id).build())))
                .containsExactly(new EnqueueResult.Accepted(id));
    }

    @Test
    void aHeldKeyRejectsARetryWithoutIt() {
        var client = builder().build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(VOID).uniqueKey("k").id(id).build());

        assertThatThrownBy(() -> client.enqueue(
                        TaskRequest.of(VOID).uniqueKey("other").id(id).build()))
                .isInstanceOf(TaskIdReusedException.class);
        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).id(id).build()))
                .isInstanceOf(TaskIdReusedException.class);
        assertThatThrownBy(() ->
                        client.enqueueAll(List.of(TaskRequest.of(VOID).id(id).build())))
                .isInstanceOf(TaskIdReusedException.class);
    }

    @Test
    void storeDownOnAColdClientThrowsEnqueueFailedWithoutIds() {
        var down = new AtomicBoolean();
        var client = TaskClient.builder()
                .queues(QUEUES)
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
                .queues(QUEUES)
                .store(failingStore(down))
                .jsonMapper(new JsonMapperTest())
                .clock(clock)
                .build();
        long id = store.allocateIds(1);
        down.set(true);

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).id(id).build()))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).containsExactly(id));
        assertThatThrownBy(() ->
                        client.enqueueAll(List.of(TaskRequest.of(VOID).id(id).build())))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).isEmpty());
    }

    @Test
    void aSucceededTaskWithoutPayloadAcceptsDifferentPayloadInBothCalls() {
        var client = builder().build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(TYPED, new Payload("original")).id(id).build());
        var leased = store.claim(new ClaimRequest(
                        List.of(new QueueGrant(store.queueId("Q:DEF"), 1, 1)),
                        1,
                        Duration.ofSeconds(30),
                        store.allocateRunnerId("test", Set.of(), Set.of()),
                        null))
                .getFirst();
        assertThat(store.ack(id, leased.revision(), true, false, null)).isTrue();
        assertThat(store.task(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(store.task(id).orElseThrow().encodedPayload()).isNull();
        var retry = TaskRequest.of(TYPED, new Payload("different")).id(id).build();

        assertThat(client.enqueue(retry)).isEqualTo(new EnqueueResult.Accepted(id));
        assertThat(client.enqueueAll(List.of(retry))).containsExactly(new EnqueueResult.Accepted(id));
        assertThat(client.counts().get("Q:DEF").succeeded()).isEqualTo(1);
        assertThat(client.counts().get("Q:DEF").queued()).isZero();
    }

    @Test
    void callerContentReadFailureCarriesIdsBeforeAnyBatchWrite() {
        var client = builder().build();
        long id = client.nextId();
        client.enqueue(TaskRequest.of(VOID).id(id).build());
        var failing = clientOn(storeFailingOn("task"));

        assertThatThrownBy(() -> failing.enqueue(TaskRequest.of(VOID).id(id).build()))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).containsExactly(id);
                    assertThat(e.storedCount()).isZero();
                });
        assertThatThrownBy(() -> failing.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(),
                        TaskRequest.of(VOID).id(id).build())))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).hasSize(2);
                    assertThat(e.notStored()).containsExactly(0);
                    assertThat(e.ids().get(1)).isEqualTo(id);
                    assertThat(e.storedCount()).isZero();
                });
        assertThat(client.counts().get("Q:DEF").queued()).isEqualTo(1);
    }

    @Test
    void generatedRequestsDoNotReadTheMarkOrStoredContent() {
        var client = clientOn(storeFailingOn("idHighWaterMark", "task"));

        assertThat(client.enqueue(TaskRequest.of(VOID).build()).isAccepted()).isTrue();
        assertThat(client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(), TaskRequest.of(VOID).build())))
                .hasSize(2)
                .allMatch(EnqueueResult::isAccepted);
    }

    @Test
    void callerIdsBelowTheLocalBlockEndNeedNoHighWaterRead() {
        long earlier = store.allocateIds(1);
        var client = clientOn(storeFailingOn("idHighWaterMark"));
        long local = client.nextId();
        var requests = List.of(
                TaskRequest.of(VOID).id(earlier).build(),
                TaskRequest.of(VOID).id(local).build());

        assertThat(client.enqueue(requests.getFirst()).isAccepted()).isTrue();
        assertThat(client.enqueueAll(requests)).hasSize(2).allMatch(EnqueueResult::isAccepted);
        assertThatThrownBy(() -> client.enqueue(
                        TaskRequest.of(VOID).id(store.idHighWaterMark()).build()))
                .isInstanceOf(EnqueueFailedException.class);
    }

    @Test
    void batchReadsEachExistingCallerIdOnceBeforeWriting() {
        var client = builder().build();
        long first = client.nextId();
        long second = client.nextId();
        var requests = List.of(
                TaskRequest.of(VOID).id(first).build(),
                TaskRequest.of(VOID).id(second).build());
        client.enqueueAll(requests);
        var reads = new ArrayList<Long>();
        var marks = new AtomicInteger();
        var observed = clientOn(interceptStore((method, args) -> {
            if (method.equals("idHighWaterMark")) marks.incrementAndGet();
            if (method.equals("task")) reads.add((Long) args[0]);
            if (method.equals("enqueueAll")) assertThat(reads).containsExactly(first, second);
        }));

        assertThat(observed.enqueueAll(requests)).hasSize(2).allMatch(EnqueueResult::isAccepted);
        assertThat(reads).containsExactly(first, second);
        assertThat(marks.get()).isEqualTo(1);
    }

    @Test
    void coldTypeLookupFailureHandsBackNoGeneratedIdForReuse() {
        var client = clientOn(storeFailingOn("typeId"));

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).build()))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).isEmpty();
                    assertThat(e.storedCount()).isZero();
                });
        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(), TaskRequest.of(VOID).build())))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).containsExactly(2L, 3L);
                    assertThat(e.notStored()).containsExactlyInAnyOrder(0, 1);
                    assertThat(e.storedCount()).isZero();
                });
        assertThat(store.task(1)).isEmpty();
        assertThat(store.task(2)).isEmpty();
    }

    @Test
    void exhaustedIdBlockFailureHasNoIdsInBothCalls() {
        var down = new AtomicBoolean();
        var client = builder().store(failingStore(down)).idBlockSize(1).build();
        client.nextId();
        down.set(true);

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).build()))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).isEmpty());
        assertThatThrownBy(() -> client.enqueueAll(List.of(TaskRequest.of(VOID).build())))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).isEmpty());
    }

    @Test
    void laterChunkFailureCarriesAllIdsAndRetriesWithoutDuplicatingStoredChunks() {
        var calls = new AtomicInteger();
        var failing = clientOn(interceptStore((method, args) -> {
            if (method.equals("enqueueAll") && calls.incrementAndGet() == 2) {
                @SuppressWarnings("unchecked")
                var records = (List<TaskRecord>) args[0];
                store.enqueue(records.getFirst());
                throw new TaskStoreException("second chunk failed");
            }
        }));
        long holder = builder()
                .build()
                .enqueue(TaskRequest.of(VOID).uniqueKey("held").build())
                .id();
        long callerId = failing.nextId();
        long laterCallerId = failing.nextId();
        var requests = new ArrayList<TaskRequest<Void>>();
        for (int i = 0; i < 1002; i++) requests.add(TaskRequest.of(VOID).build());
        requests.set(0, TaskRequest.of(VOID).uniqueKey("held").build());
        requests.set(500, TaskRequest.of(VOID).id(callerId).build());
        requests.set(1001, TaskRequest.of(VOID).id(laterCallerId).build());

        assertThatThrownBy(() -> failing.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).hasSize(requests.size()).doesNotHaveDuplicates();
                    assertThat(e.ids().get(500)).isEqualTo(callerId);
                    assertThat(e.storedCount()).isEqualTo(500);
                    assertThat(e.settled()).hasSize(500);
                    assertThat(e.settled().getFirst()).isEqualTo(new EnqueueResult.Duplicate(holder));
                    assertThat(e.settled().subList(1, 500)).allMatch(EnqueueResult::isAccepted);
                    assertThat(e.notStored()).containsExactly(1000);
                    assertThat(store.counts().get(store.queueId("Q:DEF")).queued())
                            .isEqualTo(501);
                    var retry = e.retryRequests(requests);
                    assertThat(retry).hasSize(502);
                    for (int i = 0; i < 500; i++)
                        assertThat(retry.get(i).id()).isEqualTo(e.ids().get(500 + i));
                    assertThat(retry.get(500).id()).isNull();
                    assertThat(retry.get(501).id()).isEqualTo(laterCallerId);
                    assertThat(failing.enqueueAll(retry)).hasSize(502).allMatch(EnqueueResult::isAccepted);
                });
        assertThat(store.counts().get(store.queueId("Q:DEF")).queued()).isEqualTo(requests.size());
    }

    @Test
    void lostBatchReplyPreservesTheIdsOfTasksAlreadyStored() {
        var failing = clientOn(interceptStore((method, args) -> {
            if (method.equals("enqueueAll")) {
                @SuppressWarnings("unchecked")
                var records = (List<TaskRecord>) args[0];
                store.enqueueAll(records);
                throw new TaskStoreException("reply lost");
            }
        }));
        var healthy = builder().build();
        var requests =
                List.of(TaskRequest.of(VOID).build(), TaskRequest.of(VOID).build());

        assertThatThrownBy(() -> failing.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).containsExactly(1L, 2L);
                    assertThat(e.notStored()).isEmpty();
                    assertThat(e.storedCount()).isZero();
                    assertThat(healthy.enqueueAll(e.retryRequests(requests)))
                            .extracting(EnqueueResult::id)
                            .containsExactly(1L, 2L);
                });
        assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(2);
    }

    @Test
    void generatedIdsCannotOverlapACallerIdFromTheSameBatch() {
        var client = builder().idBlockSize(10).build();
        client.nextId();

        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(),
                        TaskRequest.of(TYPED, new Payload("caller")).id(2).build())))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("overlaps");
        assertThat(store.task(2)).isEmpty();
        assertThat(store.counts()).isEmpty();
    }

    @Test
    void batchReservationCannotMakeAnUnallocatedCallerIdValid() {
        var client = builder().idBlockSize(1).build();
        long unallocated = store.idHighWaterMark();

        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(),
                        TaskRequest.of(VOID).id(unallocated).build())))
                .isInstanceOf(TaskValidationException.class);
        assertThat(store.task(unallocated)).isEmpty();
    }

    @Test
    void aSingleGeneratedTaskStopsAfterThreeCollisionsWithoutReturningAnOccupiedId() {
        var writes = new AtomicInteger();
        var client = clientOn(interceptStore((method, args) -> {
            if (method.equals("enqueue")) {
                writes.incrementAndGet();
                store.enqueue((TaskRecord) args[0]);
            }
        }));

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).build()))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).isEmpty();
                    assertThat(e.storedCount()).isZero();
                    assertThat(e.getCause()).isInstanceOf(TaskStoreException.class);
                });
        assertThat(writes.get()).isEqualTo(3);
    }

    @Test
    void reservationFailureAfterASingleCollisionDoesNotReturnTheOccupiedId() {
        var collided = new AtomicBoolean();
        var client = builder()
                .idBlockSize(1)
                .store(interceptStore((method, args) -> {
                    if (method.equals("allocateIds") && collided.get()) throw new TaskStoreException("store down");
                    if (method.equals("enqueue")) {
                        store.enqueue((TaskRecord) args[0]);
                        collided.set(true);
                    }
                }))
                .build();

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).build()))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).isEmpty();
                    assertThat(e.storedCount()).isZero();
                });
        assertThat(collided.get()).isTrue();
    }

    @Test
    void lostSingleReplyCarriesTheIdThatWasWritten() {
        var failing = clientOn(interceptStore((method, args) -> {
            if (method.equals("enqueue")) {
                store.enqueue((TaskRecord) args[0]);
                throw new TaskStoreException("reply lost");
            }
        }));
        var healthy = builder().build();

        assertThatThrownBy(() -> failing.enqueue(TaskRequest.of(VOID).build()))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).containsExactly(1L);
                    assertThat(e.storedCount()).isZero();
                    assertThat(healthy.enqueue(TaskRequest.of(VOID).id(e.id()).build()))
                            .isEqualTo(new EnqueueResult.Accepted(1));
                });
        assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(1);
    }

    @Test
    void duplicateCallerIdsAreRejectedBeforeAnyStoreCall() {
        var client = clientOn(storeFailingOn("idHighWaterMark", "allocateIds", "task", "enqueueAll"));

        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).id(1).build(),
                        TaskRequest.of(VOID).id(1).build())))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("more than one");
    }

    @Test
    void reservationFailureInAMixedBatchRetriesTheSameRequestsKeepingCallerIds() {
        var healthy = builder().idBlockSize(1).build();
        long callerId = healthy.nextId();
        var existing = TaskRequest.of(VOID).id(callerId).build();
        healthy.enqueue(existing);
        var calls = new ArrayList<String>();
        var failing = clientOn(interceptStore((method, args) -> {
            if (method.equals("idHighWaterMark") || method.equals("allocateIds")) calls.add(method);
            if (method.equals("allocateIds")) throw new TaskStoreException("store down");
        }));
        var requests = List.of(TaskRequest.of(VOID).build(), existing);

        assertThatThrownBy(() -> failing.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).isEmpty();
                    assertThat(e.notStored()).containsExactly(0);
                    assertThat(e.retryRequests(requests)).containsExactlyElementsOf(requests);
                    assertThat(e.storedCount()).isZero();
                    assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(1);
                });
        assertThat(calls).containsExactly("idHighWaterMark", "allocateIds");
        var results = healthy.enqueueAll(requests);
        assertThat(results).hasSize(2).allMatch(EnqueueResult::isAccepted);
        assertThat(results.get(1).id()).isEqualTo(callerId);
        assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(2);
    }

    @Test
    void coldNameFailuresAfterReservationCarryEveryIdInAMixedBatch() {
        long callerId = store.allocateIds(1);
        for (String name : List.of("queueId", "typeId")) {
            long firstGenerated = store.idHighWaterMark();
            var client = clientOn(storeFailingOn(name));

            assertThatThrownBy(() -> client.enqueueAll(List.of(
                            TaskRequest.of(VOID).build(),
                            TaskRequest.of(VOID).id(callerId).build())))
                    .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                        assertThat(e.ids()).containsExactly(firstGenerated, callerId);
                        assertThat(e.storedCount()).isZero();
                        assertThat(e.notStored()).containsExactly(0);
                    });
            assertThat(store.task(firstGenerated)).isEmpty();
        }
        assertThat(store.task(callerId)).isEmpty();
    }

    @Test
    void batchCollisionRetriesStopAfterThreeWritesWithoutReservingAnotherReplacement() {
        var allocations = new AtomicInteger();
        var sentIds = new ArrayList<List<Long>>();
        var client = clientOn(interceptStore((method, args) -> {
            if (method.equals("allocateIds")) allocations.incrementAndGet();
            if (method.equals("enqueueAll")) {
                @SuppressWarnings("unchecked")
                var records = (List<TaskRecord>) args[0];
                sentIds.add(records.stream().map(TaskRecord::id).toList());
                records.stream().filter(r -> "collision".equals(r.account())).forEach(store::enqueue);
            }
        }));
        var requests = List.of(
                TaskRequest.of(VOID).build(),
                TaskRequest.of(VOID).account("collision").build(),
                TaskRequest.of(VOID).build());

        assertThatThrownBy(() -> client.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).containsExactly(1L, 129L, 3L);
                    assertThat(e.notStored()).containsExactly(1);
                    assertThat(allocations).hasValue(3);
                    assertThat(e.storedCount()).isZero();
                    assertThat(e.getCause()).hasMessageContaining("3 tries");
                    var retry = builder().build().enqueueAll(e.retryRequests(requests));
                    assertThat(retry).hasSize(3).allMatch(EnqueueResult::isAccepted);
                    assertThat(retry.get(0).id()).isEqualTo(1);
                    assertThat(retry.get(1).id()).isGreaterThan(129);
                    assertThat(retry.get(2).id()).isEqualTo(3);
                });
        assertThat(sentIds).containsExactly(List.of(1L, 2L, 3L), List.of(65L), List.of(129L));
    }

    @Test
    void collisionExhaustionMarksGeneratedIdsInUnattemptedChunksButKeepsCallerIds() {
        var writes = new AtomicInteger();
        var lastTried = new java.util.concurrent.atomic.AtomicLong();
        var client = clientOn(interceptStore((method, args) -> {
            if (method.equals("enqueueAll")) {
                @SuppressWarnings("unchecked")
                var records = (List<TaskRecord>) args[0];
                writes.incrementAndGet();
                lastTried.set(records.getFirst().id());
                store.enqueue(records.getFirst());
            }
        }));
        long caller = store.allocateIds(1);
        var requests = new ArrayList<TaskRequest<Void>>();
        for (int i = 0; i < 501; i++) requests.add(TaskRequest.of(VOID).build());
        requests.add(TaskRequest.of(VOID).id(caller).build());

        assertThatThrownBy(() -> client.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).hasSize(502).doesNotHaveDuplicates();
                    assertThat(e.ids().getFirst()).isEqualTo(lastTried.get());
                    assertThat(e.ids().get(500)).isEqualTo(502L);
                    assertThat(e.ids().getLast()).isEqualTo(caller);
                    assertThat(e.notStored()).containsExactlyInAnyOrder(0, 500);
                    var retry = e.retryRequests(requests);
                    assertThat(retry.get(0).id()).isNull();
                    assertThat(retry.get(500).id()).isNull();
                    assertThat(retry.getLast().id()).isEqualTo(caller);
                    var healthy = builder().build();
                    var results = healthy.enqueueAll(retry);
                    assertThat(results).hasSize(502).allMatch(EnqueueResult::isAccepted);
                    assertThat(results.get(0).id()).isNotIn(e.ids());
                    assertThat(results.get(500).id()).isNotIn(e.ids());
                    assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(505);
                });
        assertThat(writes).hasValue(3);
    }

    @Test
    void replacementReservationFailureMarksTheCollisionAndRetryStoresADistinctTask() {
        var collided = new AtomicBoolean();
        var client = builder()
                .idBlockSize(1)
                .store(interceptStore((method, args) -> {
                    if (method.equals("allocateIds") && collided.get()) throw new TaskStoreException("store down");
                    if (method.equals("enqueueAll")) {
                        @SuppressWarnings("unchecked")
                        var records = (List<TaskRecord>) args[0];
                        store.enqueue(records.get(1));
                        collided.set(true);
                    }
                }))
                .build();
        var requests = List.of(
                TaskRequest.of(TYPED, new Payload("A")).build(),
                TaskRequest.of(TYPED, new Payload("B")).build());

        assertThatThrownBy(() -> client.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).containsExactly(1L, 2L);
                    assertThat(e.notStored()).containsExactly(1);
                    assertThat(e.storedCount()).isZero();
                    assertThat(e.getCause()).hasMessageContaining("occupied generated task ids");
                    assertThat(e.getCause().getCause()).hasMessage("store down");
                    var a = store.task(1).orElseThrow();
                    var x = store.task(2).orElseThrow();
                    var retry = e.retryRequests(requests);
                    assertThat(retry.getFirst().id()).isEqualTo(1L);
                    assertThat(retry.get(1)).isSameAs(requests.get(1));
                    var healthy = builder().build();
                    var results = healthy.enqueueAll(retry);
                    assertThat(results).allMatch(EnqueueResult::isAccepted);
                    assertThat(results.getFirst().id()).isEqualTo(1);
                    assertThat(results.get(1).id()).isNotIn(1L, 2L);
                    assertThat(store.task(results.get(1).id()).orElseThrow().encodedPayload())
                            .isEqualTo(x.encodedPayload());
                    assertThat(store.task(1).orElseThrow()).isEqualTo(a);
                    assertThat(store.task(2).orElseThrow()).isEqualTo(x);
                    assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(3);
                });
    }

    @Test
    void failureWritingAReplacementClearsTheCollisionMarkAndKeepsTheReplacementId() {
        var writes = new AtomicInteger();
        var client = clientOn(interceptStore((method, args) -> {
            if (method.equals("enqueueAll")) {
                @SuppressWarnings("unchecked")
                var records = (List<TaskRecord>) args[0];
                if (writes.incrementAndGet() == 1) store.enqueue(records.get(1));
                else {
                    store.enqueueAll(records);
                    throw new TaskStoreException("reply lost");
                }
            }
        }));
        var requests =
                List.of(TaskRequest.of(VOID).build(), TaskRequest.of(VOID).build());

        assertThatThrownBy(() -> client.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).containsExactly(1L, 65L);
                    assertThat(e.notStored()).isEmpty();
                    var healthy = builder().build();
                    assertThat(healthy.enqueueAll(e.retryRequests(requests)))
                            .extracting(EnqueueResult::id)
                            .containsExactly(1L, 65L);
                    assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(3);
                });
        assertThat(writes).hasValue(2);
    }

    @Test
    void replacementOverlapMarksCollisionsAndUnattemptedGeneratedRequests() {
        long callerId = store.allocateIds(1);
        var collided = new AtomicBoolean();
        FullStore overlapping = (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("allocateIds") && collided.get()) return callerId;
                    if (method.getName().equals("enqueueAll")) {
                        @SuppressWarnings("unchecked")
                        var records = (List<TaskRecord>) args[0];
                        store.enqueue(records.get(1));
                        collided.set(true);
                    }
                    try {
                        return method.invoke(store, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        var client = clientOn(overlapping);
        var requests = new ArrayList<TaskRequest<Void>>();
        for (int i = 0; i < 501; i++) requests.add(TaskRequest.of(VOID).build());
        requests.add(TaskRequest.of(VOID).id(callerId).build());

        assertThatThrownBy(() -> client.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.getCause()).hasMessage("replacement task ids overlap a caller id");
                    assertThat(e.notStored()).containsExactlyInAnyOrder(1, 500);
                    assertThat(e.ids().getLast()).isEqualTo(callerId);
                    var x = store.task(e.ids().get(1)).orElseThrow();
                    var retry = e.retryRequests(requests);
                    assertThat(retry.get(1).id()).isNull();
                    assertThat(retry.get(500).id()).isNull();
                    assertThat(retry.getLast().id()).isEqualTo(callerId);
                    var healthy = builder().build();
                    assertThat(healthy.enqueueAll(retry)).hasSize(502).allMatch(EnqueueResult::isAccepted);
                    assertThat(store.task(x.id()).orElseThrow()).isEqualTo(x);
                    assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(503);
                });
    }

    @Test
    void collisionsAfterAThrowingCallerResultAreStillMarked() {
        long callerId = store.allocateIds(1);
        var client = clientOn(interceptStore((method, args) -> {
            if (method.equals("enqueueAll")) {
                @SuppressWarnings("unchecked")
                var records = (List<TaskRecord>) args[0];
                store.enqueue(records.getFirst().toBuilder()
                        .encodedPayload("other payload")
                        .build());
                store.enqueue(records.get(1));
            }
        }));
        var requests = List.of(
                TaskRequest.of(VOID).id(callerId).build(),
                TaskRequest.of(VOID).build(),
                TaskRequest.of(VOID).build());

        assertThatThrownBy(() -> client.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.getCause()).isInstanceOf(TaskIdReusedException.class);
                    assertThat(e.notStored()).containsExactly(1);
                    var retry = e.retryRequests(requests);
                    assertThat(retry.getFirst().id()).isEqualTo(callerId);
                    assertThat(retry.get(1).id()).isNull();
                    assertThat(retry.get(2).id()).isEqualTo(e.ids().get(2));
                    assertThat(store.task(e.ids().get(2))).isPresent();
                });
    }

    @Test
    void batchRetriesPreserveEncodedPayloadAndRunTime() {
        var encodes = new AtomicInteger();
        var type = TaskType.of("encode-once", new PayloadCodec<String>() {
            @Override
            public String encode(String payload) {
                return payload + encodes.incrementAndGet();
            }

            @Override
            public String decode(String text) {
                return text;
            }
        });
        var writes = new AtomicInteger();
        var client = clientOn(interceptStore((method, args) -> {
            if (method.equals("enqueueAll") && writes.incrementAndGet() == 1) {
                @SuppressWarnings("unchecked")
                var records = (List<TaskRecord>) args[0];
                store.enqueue(records.getFirst());
                clock.advance(Duration.ofMinutes(1));
            }
        }));
        var due = clock.instant().plus(Duration.ofHours(1));

        var result = client.enqueueAll(List.of(
                TaskRequest.of(type, "payload").delay(Duration.ofHours(1)).build()));

        assertThat(result).containsExactly(new EnqueueResult.Accepted(65));
        var task = store.task(result.getFirst().id()).orElseThrow();
        assertThat(task.encodedPayload()).isEqualTo("payload1");
        assertThat(task.dueAt()).isEqualTo(due);
        assertThat(encodes.get()).isEqualTo(1);
        assertThat(writes.get()).isEqualTo(2);
    }

    @Test
    void callerIdTakenAfterPreflightKeepsTheIdsOfEarlierChunks() {
        var writes = new AtomicInteger();
        var client = clientOn(interceptStore((method, args) -> {
            if (method.equals("enqueueAll") && writes.incrementAndGet() == 2) {
                @SuppressWarnings("unchecked")
                var records = (List<TaskRecord>) args[0];
                store.enqueue(records.getFirst().toBuilder()
                        .encodedPayload("other payload")
                        .build());
            }
        }));
        long callerId = client.nextId();
        var requests = new ArrayList<TaskRequest<Void>>();
        for (int i = 0; i < 500; i++) requests.add(TaskRequest.of(VOID).build());
        requests.add(TaskRequest.of(VOID).id(callerId).build());

        assertThatThrownBy(() -> client.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).hasSize(501).doesNotHaveDuplicates();
                    assertThat(e.ids().getLast()).isEqualTo(callerId);
                    assertThat(e.storedCount()).isEqualTo(500);
                    assertThat(e.getCause()).isInstanceOf(TaskIdReusedException.class);
                    assertThat(store.task(e.ids().getFirst())).isPresent();
                });
    }

    @Test
    void aCallerIdWhoseStoredTaskCannotBeReadIsThrownAsItIsAndNothingIsWritten() {
        var unreadable = new AtomicBoolean();
        var client = clientOn(interceptStore((method, args) -> {
            if (unreadable.get() && method.equals("task")) throw unreadable((long) args[0]);
        }));
        long id = client.nextId();
        var request = TaskRequest.of(VOID).id(id).build();
        client.enqueue(request);
        unreadable.set(true);

        assertThatThrownBy(() -> client.enqueue(request))
                .isInstanceOfSatisfying(
                        UnreadableTaskException.class,
                        e -> assertThat(e.taskId()).isEqualTo(id));
        assertThatThrownBy(() -> client.enqueueAll(List.of(TaskRequest.of(VOID).build(), request)))
                .isInstanceOfSatisfying(
                        UnreadableTaskException.class,
                        e -> assertThat(e.taskId()).isEqualTo(id));
        assertThat(store.tasks(TaskState.QUEUED, null, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(id);
    }

    @Test
    void whateverFailsInALaterChunkTheRecoveryDataIsKept() {
        List<RuntimeException> failures = List.of(unreadable(7), new IllegalStateException("not a bth4j failure"));
        for (var failure : failures) {
            var writes = new AtomicInteger();
            var client = clientOn(interceptStore((method, args) -> {
                if (method.equals("enqueueAll") && writes.incrementAndGet() == 2) throw failure;
            }));
            var requests = new ArrayList<TaskRequest<Void>>();
            for (int i = 0; i < 501; i++) requests.add(TaskRequest.of(VOID).build());

            assertThatThrownBy(() -> client.enqueueAll(requests))
                    .as("%s", failure)
                    .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                        assertThat(e.getCause()).isSameAs(failure);
                        assertThat(e.ids()).hasSize(501).doesNotHaveDuplicates();
                        assertThat(e.settled()).hasSize(500);
                        assertThat(e.retryRequests(requests))
                                .singleElement()
                                .satisfies(r ->
                                        assertThat(r.id()).isEqualTo(e.ids().getLast()));
                    });
        }
    }

    private static UnreadableTaskException unreadable(long id) {
        return new UnreadableTaskException(id, "task " + id + " has a stored value that cannot be read", null);
    }

    @Test
    void replacementIdsCannotTakeACallerIdFromALaterChunk() {
        var client = builder().idBlockSize(1000).build();
        client.nextId();
        long callerId = 502;
        var other = builder().build();
        other.enqueue(TaskRequest.of(VOID).id(2).build());
        var requests = new ArrayList<TaskRequest<Void>>();
        for (int i = 0; i < 500; i++) requests.add(TaskRequest.of(VOID).build());
        requests.add(TaskRequest.of(VOID).id(callerId).build());

        var results = client.enqueueAll(requests);
        assertThat(results).hasSize(501).allMatch(EnqueueResult::isAccepted);
        assertThat(results).extracting(EnqueueResult::id).doesNotHaveDuplicates();
        assertThat(results.getFirst().id()).isEqualTo(1001);
        assertThat(results.getLast().id()).isEqualTo(callerId);
        assertThat(store.task(callerId)).isPresent();
    }

    @Test
    void uniqueKeyAnswersDuplicateWithTheHolder() {
        var client = builder().build();

        var first = client.enqueue(TaskRequest.of(VOID).uniqueKey("k").build());
        var second = client.enqueue(TaskRequest.of(VOID).uniqueKey("k").build());
        var other = client.enqueue(TaskRequest.of(VOID).uniqueKey("other").build());

        assertThat(first.isAccepted()).isTrue();
        assertThat(second).isEqualTo(new EnqueueResult.Duplicate(first.id()));
        assertThat(second.isAccepted()).isFalse();
        assertThat(second.id()).isEqualTo(first.id());
        assertThat(other.isAccepted()).isTrue();
        assertThat(client.tasks(TaskState.QUEUED, null, null, 100).items()).hasSize(2);
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

        assertThat(results).hasSize(1200).allMatch(EnqueueResult::isAccepted);
        assertThat(results.stream().map(EnqueueResult::id).distinct().count()).isEqualTo(1200L);
        assertThat(client.counts().get("Q:DEF").queued()).isEqualTo(1200L);
    }

    @Test
    void headersAtTheLimitAreStoredAndOverTheLimitAreRejected() {
        var client = builder().build();
        var fits = "x".repeat(TaskRequest.MAX_HEADERS_BYTES - 1);

        var accepted = client.enqueue(TaskRequest.of(VOID).addHeader("h", fits).build());

        assertThat(client.task(accepted.id()).orElseThrow().headers()).containsEntry("h", fits);
        assertThatThrownBy(() -> client.enqueue(
                        TaskRequest.of(VOID).addHeader("h", fits + "x").build()))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("headers");
    }

    @Test
    void headerLimitCountsUtf8BytesOfNamesAndValues() {
        var client = builder().build();
        var twoBytes = "é".repeat(TaskRequest.MAX_HEADERS_BYTES / 4);
        var request = TaskRequest.of(VOID)
                .addHeader("a", twoBytes)
                .addHeader("b", twoBytes)
                .addHeader("c", "x")
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
        assertThat(client.tasks(TaskState.QUEUED, null, null, 10).items()).isEmpty();
    }

    @Test
    void payloadBelowTheLimitIsStoredEncoded() {
        var client = builder().build();
        var big = new Payload("x".repeat(TaskRequest.MAX_PAYLOAD_BYTES - 100));

        var result = client.enqueue(TaskRequest.of(TYPED, big).build());

        assertThat(client.task(result.id()).orElseThrow().encodedPayload())
                .hasSize(TaskRequest.MAX_PAYLOAD_BYTES - 100 + 11);
    }

    @Test
    void taskViewResolvesQueueAndTypeNames() {
        var client = builder().build();

        var result = client.enqueue(TaskRequest.of(TYPED, new Payload("v"))
                .queue("Q:HIGH")
                .account("acme")
                .maxAttempts(4)
                .build());

        var view = client.task(result.id()).orElseThrow();
        assertThat(view.id()).isEqualTo(result.id());
        assertThat(view.queue()).isEqualTo("Q:HIGH");
        assertThat(view.typeName()).isEqualTo("client-typed");
        assertThat(view.state()).isEqualTo(TaskState.QUEUED);
        assertThat(view.account()).isEqualTo("acme");
        assertThat(view.maxAttempts()).isEqualTo(4);
        assertThat(view.attempts()).isZero();
        assertThat(view.payloadVersion()).isEqualTo(1);
        assertThat(view.encodedPayload()).contains("\"text\":\"v\"");
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
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void aClientWithoutQueuesHasTheOneQueueCommon() {
        var client = TaskClient.builder().store(store).clock(clock).build();

        var view =
                client.task(client.enqueue(TaskRequest.of(VOID).build()).id()).orElseThrow();

        assertThat(view.queue()).isEqualTo("general");
        assertThatThrownBy(
                        () -> client.enqueue(TaskRequest.of(VOID).queue("Q:DEF").build()))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void theQueuesAClientIsGivenReplaceTheBuiltInOneAndNameTheDefault() {
        var client = builder().queues(Queues.of("A", "B").withDefault("B")).build();

        var defaulted =
                client.task(client.enqueue(TaskRequest.of(VOID).build()).id()).orElseThrow();

        assertThat(defaulted.queue()).isEqualTo("B");
        assertThatThrownBy(
                        () -> client.enqueue(TaskRequest.of(VOID).queue("Q:DEF").build()))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void delayedTaskIsScheduledAtTheClientClockPlusDelay() {
        var client = builder().build();

        var result =
                client.enqueue(TaskRequest.of(VOID).delay(Duration.ofMinutes(5)).build());

        var view = client.task(result.id()).orElseThrow();
        assertThat(view.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(view.dueAt()).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
    }

    @Test
    void startByInThePastIsRejectedForATaskThatRunsAtOnce() {
        var client = builder().build();
        var past = TaskRequest.of(VOID).startBy(clock.instant().minusMillis(1)).build();
        var now = TaskRequest.of(VOID).startBy(clock.instant()).build();

        assertThatThrownBy(() -> client.enqueue(past))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("startBy is before the run time");
        assertThat(client.enqueue(now)).isInstanceOf(EnqueueResult.Accepted.class);
    }

    @Test
    void aResendWithACallerIdIsAcceptedAfterItsStartByPassedOrItsDelayMovedPastIt() {
        var client = builder().build();
        var atOnce = TaskRequest.of(VOID)
                .id(client.nextId())
                .startBy(clock.instant().plusSeconds(5))
                .build();
        var delayed = TaskRequest.of(VOID)
                .id(client.nextId())
                .delay(Duration.ofSeconds(60))
                .startBy(clock.instant().plusSeconds(90))
                .build();
        var neverSent = TaskRequest.of(VOID)
                .id(client.nextId())
                .startBy(clock.instant().plusSeconds(5))
                .build();
        assertThat(client.enqueue(atOnce)).isEqualTo(new EnqueueResult.Accepted(atOnce.id()));
        assertThat(client.enqueue(delayed)).isEqualTo(new EnqueueResult.Accepted(delayed.id()));

        clock.advance(Duration.ofSeconds(45));

        assertThat(client.enqueue(atOnce)).isEqualTo(new EnqueueResult.Accepted(atOnce.id()));
        assertThat(client.enqueue(delayed)).isEqualTo(new EnqueueResult.Accepted(delayed.id()));
        assertThat(client.enqueueAll(List.of(atOnce, delayed)))
                .containsExactly(new EnqueueResult.Accepted(atOnce.id()), new EnqueueResult.Accepted(delayed.id()));
        assertThatThrownBy(() -> client.enqueue(neverSent))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("startBy is before the run time");
    }

    @Test
    void aResendIsAcceptedAfterItsStartByPassedAlsoWhenTheTaskWasRemovedAndOnlyItsKeyIsKept() {
        var client = builder().build();
        var request = TaskRequest.of(VOID)
                .id(client.nextId())
                .uniqueKey("order-7", Uniqueness.keepFor(Duration.ofHours(1)))
                .startBy(clock.instant().plusSeconds(5))
                .build();
        var otherKey = TaskRequest.of(VOID)
                .id(request.id())
                .uniqueKey("order-8", Uniqueness.keepFor(Duration.ofHours(1)))
                .startBy(clock.instant().plusSeconds(5))
                .build();
        assertThat(client.enqueue(request)).isEqualTo(new EnqueueResult.Accepted(request.id()));
        var leased = store.claim(new ClaimRequest(
                        List.of(new QueueGrant(store.queueId("Q:DEF"), 1, 1)),
                        1,
                        Duration.ofSeconds(30),
                        store.allocateRunnerId("test", Set.of(), Set.of()),
                        null))
                .getFirst();
        assertThat(store.ack(request.id(), leased.revision(), false, false, null))
                .isTrue();
        assertThat(store.task(request.id())).isEmpty();

        clock.advance(Duration.ofSeconds(45));

        assertThat(client.enqueue(request)).isEqualTo(new EnqueueResult.Accepted(request.id()));
        assertThat(client.enqueueAll(List.of(request))).containsExactly(new EnqueueResult.Accepted(request.id()));
        assertThat(store.task(request.id())).as("not stored again").isEmpty();
        assertThatThrownBy(() -> client.enqueue(otherKey))
                .as("the same id with a key it does not hold")
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("startBy is before the run time");
    }

    @Test
    void aJsonPayloadWithoutAMapperIsAConfigurationError() {
        var client =
                TaskClient.builder().queues(QUEUES).store(store).clock(clock).build();

        assertThatThrownBy(() ->
                        client.enqueue(TaskRequest.of(TYPED, new Payload("a")).build()))
                .isInstanceOf(TaskConfigurationException.class);
        assertThat(client.enqueue(TaskRequest.of(VOID).build())).isInstanceOf(EnqueueResult.Accepted.class);
    }

    @Test
    void aKeyLookupWithoutAKeyAndAScheduleIdThatIsMissingOrHasAU0000AreRejected() {
        var client = builder().build();

        assertThatThrownBy(() -> client.taskByKey(null)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> client.deleteSchedule("s\u0000")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> client.deleteSchedule(null)).isInstanceOf(TaskValidationException.class);
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
    void aRunTimeOrStartByMoreThanOneHundredYearsFromNowIsRejected() {
        var client = builder().build();
        var now = clock.instant();
        var longest = TaskRequest.MAX_TIME_FROM_NOW;

        assertThat(client.enqueue(TaskRequest.of(VOID).at(now.plus(longest)).build())
                        .isAccepted())
                .isTrue();
        assertThat(client.enqueue(TaskRequest.of(VOID).at(now.minus(longest)).build())
                        .isAccepted())
                .isTrue();
        assertThat(client.enqueue(
                                TaskRequest.of(VOID).startBy(now.plus(longest)).build())
                        .isAccepted())
                .isTrue();
        for (var bad :
                List.of(now.plus(longest).plusMillis(1), now.minus(longest).minusMillis(1), Instant.MAX)) {
            assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).at(bad).build()))
                    .as("at %s", bad)
                    .isInstanceOf(TaskValidationException.class)
                    .hasMessageStartingWith("at ");
            assertThatThrownBy(() ->
                            client.enqueue(TaskRequest.of(VOID).startBy(bad).build()))
                    .as("startBy %s", bad)
                    .isInstanceOf(TaskValidationException.class)
                    .hasMessageStartingWith("startBy ");
        }
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

        var all = client.tasks(TaskState.QUEUED, "Q:DEF", null, 100).items();
        var firstPage = client.tasks(TaskState.QUEUED, "Q:DEF", null, 2);
        var secondPage =
                client.tasks(TaskState.QUEUED, "Q:DEF", firstPage.next(), 100).items();

        assertThat(all).extracting(TaskView::id).containsExactlyElementsOf(ids);
        assertThat(firstPage.items()).extracting(TaskView::id).containsExactlyElementsOf(ids.subList(0, 2));
        assertThat(secondPage).extracting(TaskView::id).containsExactlyElementsOf(ids.subList(2, 5));
        assertThat(client.tasks(TaskState.SUCCEEDED, null, null, 100).items()).isEmpty();
    }

    @Test
    void schedulePutDeleteAndList() {
        var client = builder().build();
        var definition = ScheduleDefinition.of("nightly", TYPED, new Payload("p"), "0 3 * * *", Overlap.ALLOW);

        var put = client.putSchedule(definition);

        assertThat(put)
                .isEqualTo(new PutScheduleResult(PutScheduleOutcome.CREATED, Instant.parse("2030-01-01T03:00:00Z")));
        assertThat(client.nextFireTimes()).containsEntry("nightly", put.nextFireAt());

        assertThat(client.deleteSchedule("nightly")).isTrue();
        assertThat(client.deleteSchedule("nightly")).isFalse();
        assertThat(client.nextFireTimes()).isEmpty();
    }

    @Test
    void registeringAScheduleAgainKeepsTheNextFireTimeForTheSameCronAndResetsItForAnother() {
        var client = builder().build();
        var first = client.putSchedule(ScheduleDefinition.of("s", VOID, "0 3 * * *", Overlap.ALLOW));
        clock.advance(Duration.ofHours(1));

        var same = client.putSchedule(ScheduleDefinition.of("s", VOID, "0 3 * * *", Overlap.ALLOW));
        var changed = client.putSchedule(ScheduleDefinition.of("s", VOID, "0 5 * * *", Overlap.ALLOW));

        var three = Instant.parse("2030-01-01T03:00:00Z");
        var five = Instant.parse("2030-01-01T05:00:00Z");
        assertThat(first).isEqualTo(new PutScheduleResult(PutScheduleOutcome.CREATED, three));
        assertThat(same).isEqualTo(new PutScheduleResult(PutScheduleOutcome.UNCHANGED, three));
        assertThat(changed).isEqualTo(new PutScheduleResult(PutScheduleOutcome.CHANGED, five));
        assertThat(client.nextFireTimes()).containsOnly(java.util.Map.entry("s", five));
    }

    @Test
    void registeringAFixedDelayScheduleAgainAnswersTheStoredNextFireTime() {
        var client = builder().build();
        var every = ScheduleDefinition.every("poll", VOID, Duration.ofMinutes(5));
        var first = client.putSchedule(every);
        clock.advance(Duration.ofSeconds(10));

        var again = client.putSchedule(every);

        assertThat(first.nextFireAt()).isEqualTo(Instant.parse("2030-01-01T00:05:00Z"));
        assertThat(again).isEqualTo(new PutScheduleResult(PutScheduleOutcome.UNCHANGED, first.nextFireAt()));
    }

    @Test
    void invalidCronIsRejected() {
        var client = builder().build();

        assertThatThrownBy(() -> client.putSchedule(ScheduleDefinition.of("bad", VOID, "not a cron", Overlap.ALLOW)))
                .isInstanceOf(TaskValidationException.class);
        assertThat(client.nextFireTimes()).isEmpty();
    }

    @Test
    void scheduleDefinitionValidatesItsFields() {
        assertThatThrownBy(() -> ScheduleDefinition.of(" ", VOID, "* * * * *", Overlap.ALLOW))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.of("x".repeat(129), VOID, "* * * * *", Overlap.ALLOW))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.of("id", TYPED, null, "* * * * *", Overlap.ALLOW))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.of("id", VOID, " ", Overlap.ALLOW))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.of("id", VOID, "* * * * *", Overlap.ALLOW)
                        .withMaxAttempts(0))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void builderRequiresAStoreAndValidatesItsSettings() {
        assertThatThrownBy(() -> TaskClient.builder().build()).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> TaskClient.builder().idBlockSize(0)).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> TaskClient.builder().defaultMaxAttempts(0))
                .isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> TaskClient.builder().clock(null)).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> TaskClient.builder().queues((Queues) null))
                .isInstanceOf(TaskConfigurationException.class);
    }
}
