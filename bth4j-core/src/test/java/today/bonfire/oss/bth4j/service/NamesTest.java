package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.spi.StoreInfo;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStore;

class NamesTest {

    private static final class RegistryStore implements TaskStore {
        final Map<Integer, String> queues = new LinkedHashMap<>();
        final Map<Integer, String> events = new LinkedHashMap<>();
        final AtomicInteger queueIdCalls = new AtomicInteger();
        final AtomicInteger eventIdCalls = new AtomicInteger();
        final AtomicInteger queueNamesCalls = new AtomicInteger();
        final AtomicInteger eventNamesCalls = new AtomicInteger();

        @Override
        public synchronized int queueId(String name) {
            queueIdCalls.incrementAndGet();
            return register(queues, name);
        }

        @Override
        public synchronized int eventId(String name) {
            eventIdCalls.incrementAndGet();
            return register(events, name);
        }

        synchronized int register(Map<Integer, String> registry, String name) {
            for (var e : registry.entrySet()) if (e.getValue().equals(name)) return e.getKey();
            int id = registry.size() + 1;
            registry.put(id, name);
            return id;
        }

        @Override
        public synchronized Map<Integer, String> queueNames() {
            queueNamesCalls.incrementAndGet();
            return Map.copyOf(queues);
        }

        @Override
        public synchronized Map<Integer, String> eventNames() {
            eventNamesCalls.incrementAndGet();
            return Map.copyOf(events);
        }

        @Override
        public StoreInfo info() {
            return new StoreInfo("test", StoreInfo.SCHEMA_VERSION);
        }

        @Override
        public void init() {}

        @Override
        public long allocateIds(int count) {
            return 1;
        }

        @Override
        public int allocateWorkerId(String label) {
            return 1;
        }
    }

    @Test
    void idLookupIsCachedAfterTheFirstCall() {
        var store = new RegistryStore();
        var names = new Names(store);

        int first = names.queueId("Q:DEF");
        int second = names.queueId("Q:DEF");
        names.eventId("e");
        names.eventId("e");

        assertThat(second).isEqualTo(first);
        assertThat(store.queueIdCalls).hasValue(1);
        assertThat(store.eventIdCalls).hasValue(1);
    }

    @Test
    void nameOfAnIdResolvedHereNeedsNoRegistryReload() {
        var store = new RegistryStore();
        var names = new Names(store);

        int queue = names.queueId("Q:DEF");
        int event = names.eventId("send");

        assertThat(names.queueName(queue)).isEqualTo("Q:DEF");
        assertThat(names.eventName(event)).isEqualTo("send");
        assertThat(store.queueNamesCalls).hasValue(0);
        assertThat(store.eventNamesCalls).hasValue(0);
    }

    @Test
    void idRegisteredByAnotherProcessIsLoadedOnceFromTheRegistry() {
        var store = new RegistryStore();
        int foreignQueue = store.queueId("Q:OTHER");
        int foreignEvent = store.eventId("other-event");
        var names = new Names(store);

        assertThat(names.queueName(foreignQueue)).isEqualTo("Q:OTHER");
        assertThat(names.queueName(foreignQueue)).isEqualTo("Q:OTHER");
        assertThat(names.eventName(foreignEvent)).isEqualTo("other-event");
        assertThat(names.eventName(foreignEvent)).isEqualTo("other-event");

        assertThat(store.queueNamesCalls).hasValue(1);
        assertThat(store.eventNamesCalls).hasValue(1);
    }

    @Test
    void reloadAlsoFillsTheForwardCache() {
        var store = new RegistryStore();
        int foreignQueue = store.queueId("Q:OTHER");
        var names = new Names(store);
        names.queueName(foreignQueue);
        int idCallsBefore = store.queueIdCalls.get();

        assertThat(names.queueId("Q:OTHER")).isEqualTo(foreignQueue);
        assertThat(store.queueIdCalls).hasValue(idCallsBefore);
    }

    @Test
    void unknownIdsFallBackToAPlaceholder() {
        var names = new Names(new RegistryStore());

        assertThat(names.queueName(77)).isEqualTo("#77");
        assertThat(names.eventName(78)).isEqualTo("#78");
    }

    @Test
    void viewResolvesQueueAndEventNames() {
        var store = new RegistryStore();
        var names = new Names(store);
        var record = TaskRecord.builder()
                .id(5)
                .queueId(names.queueId("Q:HIGH"))
                .eventId(names.eventId("report"))
                .state(TaskState.QUEUED)
                .build();

        var view = names.view(record);

        assertThat(view.id()).isEqualTo(5);
        assertThat(view.queue()).isEqualTo("Q:HIGH");
        assertThat(view.event()).isEqualTo("report");
        assertThat(view.state()).isEqualTo(TaskState.QUEUED);
    }
}
