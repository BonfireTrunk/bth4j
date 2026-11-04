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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.EnqueueFailedException;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.exceptions.TaskIdReusedException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskRecord;
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
        assertThat(result.id()).isEqualTo(11);
        assertThat(b.tasks(TaskState.QUEUED, null, 0, 100))
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
        assertThat(b.tasks(TaskState.QUEUED, null, 0, 100)).hasSize(4);
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
                    assertThat(e.stored()).isZero();
                });
        assertThat(allocations.get()).isEqualTo(1);
        assertThat(client.nextId()).isEqualTo(mine + 1);
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
                        List.of(store.queueId("Q:DEF")),
                        1,
                        Duration.ofSeconds(30),
                        store.allocateWorkerId("test"),
                        null))
                .getFirst();
        assertThat(store.ack(id, leased.revision(), true, false, null)).isTrue();
        assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(store.getTask(id).orElseThrow().data()).isNull();
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
        var failing = clientOn(storeFailingOn("getTask"));

        assertThatThrownBy(() -> failing.enqueue(TaskRequest.of(VOID).id(id).build()))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).containsExactly(id);
                    assertThat(e.stored()).isZero();
                });
        assertThatThrownBy(() -> failing.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(),
                        TaskRequest.of(VOID).id(id).build())))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).hasSize(2);
                    assertThat(e.notStored()).containsExactly(0);
                    assertThat(e.ids().get(1)).isEqualTo(id);
                    assertThat(e.stored()).isZero();
                });
        assertThat(client.counts().get("Q:DEF").queued()).isEqualTo(1);
    }

    @Test
    void generatedRequestsDoNotReadTheMarkOrStoredContent() {
        var client = clientOn(storeFailingOn("idHighWaterMark", "getTask"));

        assertThat(client.enqueue(TaskRequest.of(VOID).build()).accepted()).isTrue();
        assertThat(client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(), TaskRequest.of(VOID).build())))
                .hasSize(2)
                .allMatch(EnqueueResult::accepted);
    }

    @Test
    void callerIdsBelowTheLocalBlockEndNeedNoHighWaterRead() {
        long earlier = store.allocateIds(1);
        var client = clientOn(storeFailingOn("idHighWaterMark"));
        long local = client.nextId();
        var requests = List.of(
                TaskRequest.of(VOID).id(earlier).build(),
                TaskRequest.of(VOID).id(local).build());

        assertThat(client.enqueue(requests.getFirst()).accepted()).isTrue();
        assertThat(client.enqueueAll(requests)).hasSize(2).allMatch(EnqueueResult::accepted);
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
            if (method.equals("getTask")) reads.add((Long) args[0]);
            if (method.equals("enqueueAll")) assertThat(reads).containsExactly(first, second);
        }));

        assertThat(observed.enqueueAll(requests)).hasSize(2).allMatch(EnqueueResult::accepted);
        assertThat(reads).containsExactly(first, second);
        assertThat(marks.get()).isEqualTo(1);
    }

    @Test
    void coldEventLookupFailureHandsBackNoGeneratedIdForReuse() {
        var client = clientOn(storeFailingOn("eventId"));

        assertThatThrownBy(() -> client.enqueue(TaskRequest.of(VOID).build()))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).isEmpty();
                    assertThat(e.stored()).isZero();
                });
        assertThatThrownBy(() -> client.enqueueAll(List.of(
                        TaskRequest.of(VOID).build(), TaskRequest.of(VOID).build())))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).containsExactly(2L, 3L);
                    assertThat(e.notStored()).containsExactlyInAnyOrder(0, 1);
                    assertThat(e.stored()).isZero();
                });
        assertThat(store.getTask(1)).isEmpty();
        assertThat(store.getTask(2)).isEmpty();
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
        long callerId = failing.nextId();
        long laterCallerId = failing.nextId();
        var requests = new ArrayList<TaskRequest<Void>>();
        for (int i = 0; i < 1002; i++) requests.add(TaskRequest.of(VOID).build());
        requests.set(500, TaskRequest.of(VOID).id(callerId).build());
        requests.set(1001, TaskRequest.of(VOID).id(laterCallerId).build());

        assertThatThrownBy(() -> failing.enqueueAll(requests))
                .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                    assertThat(e.ids()).hasSize(requests.size()).doesNotHaveDuplicates();
                    assertThat(e.ids().get(500)).isEqualTo(callerId);
                    assertThat(e.stored()).isEqualTo(500);
                    assertThat(e.notStored()).containsExactly(1000);
                    assertThat(store.counts().get(store.queueId("Q:DEF")).queued())
                            .isEqualTo(501);
                    var retry = e.retryRequests(requests);
                    for (int i = 0; i < 1000; i++)
                        assertThat(retry.get(i).id()).isEqualTo(e.ids().get(i));
                    assertThat(retry.get(1000).id()).isNull();
                    assertThat(retry.get(1001).id()).isEqualTo(laterCallerId);
                    assertThat(failing.enqueueAll(retry))
                            .hasSize(requests.size())
                            .allMatch(EnqueueResult::accepted);
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
                    assertThat(e.stored()).isZero();
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
        assertThat(store.getTask(2)).isEmpty();
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
        assertThat(store.getTask(unallocated)).isEmpty();
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
                    assertThat(e.stored()).isZero();
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
                    assertThat(e.stored()).isZero();
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
                    assertThat(e.stored()).isZero();
                    assertThat(healthy.enqueue(TaskRequest.of(VOID).id(e.id()).build()))
                            .isEqualTo(new EnqueueResult.Accepted(1));
                });
        assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(1);
    }

    @Test
    void duplicateCallerIdsAreRejectedBeforeAnyStoreCall() {
        var client = clientOn(storeFailingOn("idHighWaterMark", "allocateIds", "getTask", "enqueueAll"));

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
                    assertThat(e.stored()).isZero();
                    assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(1);
                });
        assertThat(calls).containsExactly("idHighWaterMark", "allocateIds");
        var results = healthy.enqueueAll(requests);
        assertThat(results).hasSize(2).allMatch(EnqueueResult::accepted);
        assertThat(results.get(1).id()).isEqualTo(callerId);
        assertThat(healthy.counts().get("Q:DEF").queued()).isEqualTo(2);
    }

    @Test
    void coldNameFailuresAfterReservationCarryEveryIdInAMixedBatch() {
        long callerId = store.allocateIds(1);
        for (String name : List.of("queueId", "eventId")) {
            long firstGenerated = store.idHighWaterMark();
            var client = clientOn(storeFailingOn(name));

            assertThatThrownBy(() -> client.enqueueAll(List.of(
                            TaskRequest.of(VOID).build(),
                            TaskRequest.of(VOID).id(callerId).build())))
                    .isInstanceOfSatisfying(EnqueueFailedException.class, e -> {
                        assertThat(e.ids()).containsExactly(firstGenerated, callerId);
                        assertThat(e.stored()).isZero();
                        assertThat(e.notStored()).containsExactly(0);
                    });
            assertThat(store.getTask(firstGenerated)).isEmpty();
        }
        assertThat(store.getTask(callerId)).isEmpty();
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
                    assertThat(e.stored()).isZero();
                    assertThat(e.getCause()).hasMessageContaining("3 tries");
                    var retry = builder().build().enqueueAll(e.retryRequests(requests));
                    assertThat(retry).hasSize(3).allMatch(EnqueueResult::accepted);
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
                    assertThat(results).hasSize(502).allMatch(EnqueueResult::accepted);
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
                    assertThat(e.stored()).isZero();
                    assertThat(e.getCause()).hasMessageContaining("occupied generated task ids");
                    assertThat(e.getCause().getCause()).hasMessage("store down");
                    var a = store.getTask(1).orElseThrow();
                    var x = store.getTask(2).orElseThrow();
                    var retry = e.retryRequests(requests);
                    assertThat(retry.getFirst().id()).isEqualTo(1L);
                    assertThat(retry.get(1)).isSameAs(requests.get(1));
                    var healthy = builder().build();
                    var results = healthy.enqueueAll(retry);
                    assertThat(results).allMatch(EnqueueResult::accepted);
                    assertThat(results.getFirst().id()).isEqualTo(1);
                    assertThat(results.get(1).id()).isNotIn(1L, 2L);
                    assertThat(store.getTask(results.get(1).id()).orElseThrow().data())
                            .isEqualTo(x.data());
                    assertThat(store.getTask(1).orElseThrow()).isEqualTo(a);
                    assertThat(store.getTask(2).orElseThrow()).isEqualTo(x);
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
                    var x = store.getTask(e.ids().get(1)).orElseThrow();
                    var retry = e.retryRequests(requests);
                    assertThat(retry.get(1).id()).isNull();
                    assertThat(retry.get(500).id()).isNull();
                    assertThat(retry.getLast().id()).isEqualTo(callerId);
                    var healthy = builder().build();
                    assertThat(healthy.enqueueAll(retry)).hasSize(502).allMatch(EnqueueResult::accepted);
                    assertThat(store.getTask(x.id()).orElseThrow()).isEqualTo(x);
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
                store.enqueue(
                        records.getFirst().toBuilder().data("other payload").build());
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
                    assertThat(store.getTask(e.ids().get(2))).isPresent();
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
        var task = store.getTask(result.getFirst().id()).orElseThrow();
        assertThat(task.data()).isEqualTo("payload1");
        assertThat(task.due()).isEqualTo(due);
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
                store.enqueue(
                        records.getFirst().toBuilder().data("other payload").build());
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
                    assertThat(e.stored()).isEqualTo(500);
                    assertThat(e.getCause()).isInstanceOf(TaskIdReusedException.class);
                    assertThat(store.getTask(e.ids().getFirst())).isPresent();
                });
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
        assertThat(results).hasSize(501).allMatch(EnqueueResult::accepted);
        assertThat(results).extracting(EnqueueResult::id).doesNotHaveDuplicates();
        assertThat(results.getFirst().id()).isEqualTo(1001);
        assertThat(results.getLast().id()).isEqualTo(callerId);
        assertThat(store.getTask(callerId)).isPresent();
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
