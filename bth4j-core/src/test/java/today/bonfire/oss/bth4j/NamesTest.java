package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.spi.StoreInfo;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStore;

class NamesTest {

    private static final class RegistryStore implements TaskStore {
        final Map<Integer, String> queues = new LinkedHashMap<>();
        final Map<Integer, String> types = new LinkedHashMap<>();
        final AtomicInteger queueIdCalls = new AtomicInteger();
        final AtomicInteger typeIdCalls = new AtomicInteger();
        final AtomicInteger queueNamesCalls = new AtomicInteger();
        final AtomicInteger typeNamesCalls = new AtomicInteger();

        @Override
        public synchronized int queueId(String name) {
            queueIdCalls.incrementAndGet();
            return register(queues, name);
        }

        @Override
        public synchronized int typeId(String name) {
            typeIdCalls.incrementAndGet();
            return register(types, name);
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
        public synchronized Map<Integer, String> typeNames() {
            typeNamesCalls.incrementAndGet();
            return Map.copyOf(types);
        }

        @Override
        public StoreInfo info() {
            return new StoreInfo("test", 1);
        }

        @Override
        public void init() {}

        @Override
        public long allocateIds(int count) {
            return 1;
        }

        @Override
        public int allocateWorkerId(String label, Set<Integer> queueIds, Set<Integer> typeIds) {
            return 1;
        }
    }

    @Test
    void idLookupIsCachedAfterTheFirstCall() {
        var store = new RegistryStore();
        var names = new Names(store);

        int first = names.queueId("Q:DEF");
        int second = names.queueId("Q:DEF");
        names.typeId("e");
        names.typeId("e");

        assertThat(second).isEqualTo(first);
        assertThat(store.queueIdCalls).hasValue(1);
        assertThat(store.typeIdCalls).hasValue(1);
    }

    @Test
    void nameOfAnIdResolvedHereNeedsNoRegistryReload() {
        var store = new RegistryStore();
        var names = new Names(store);

        int queue = names.queueId("Q:DEF");
        int type = names.typeId("send");

        assertThat(names.queueName(queue)).isEqualTo("Q:DEF");
        assertThat(names.typeName(type)).isEqualTo("send");
        assertThat(store.queueNamesCalls).hasValue(0);
        assertThat(store.typeNamesCalls).hasValue(0);
    }

    @Test
    void idRegisteredByAnotherProcessIsLoadedOnceFromTheRegistry() {
        var store = new RegistryStore();
        int foreignQueue = store.queueId("Q:OTHER");
        int foreignType = store.typeId("other-type");
        var names = new Names(store);

        assertThat(names.queueName(foreignQueue)).isEqualTo("Q:OTHER");
        assertThat(names.queueName(foreignQueue)).isEqualTo("Q:OTHER");
        assertThat(names.typeName(foreignType)).isEqualTo("other-type");
        assertThat(names.typeName(foreignType)).isEqualTo("other-type");

        assertThat(store.queueNamesCalls).hasValue(1);
        assertThat(store.typeNamesCalls).hasValue(1);
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
        assertThat(names.typeName(78)).isEqualTo("#78");
    }

    @Test
    void anEventNamesARegisteredQueueAndTypeAndSaysUnknownForTheRest() {
        var store = new RegistryStore();
        var names = new Names(store);
        int queue = names.queueId("Q:DEF");
        int type = names.typeId("send");

        assertThat(names.queueNameOrUnknown(queue)).isEqualTo("Q:DEF");
        assertThat(names.typeNameOrUnknown(type)).isEqualTo("send");
        assertThat(names.queueNameOrUnknown(77)).isEqualTo("unknown");
        assertThat(names.typeNameOrUnknown(78)).isEqualTo("unknown");
    }

    @Test
    void anIdOfZeroIsUnknownWithoutAskingTheRegistry() {
        var store = new RegistryStore();
        var names = new Names(store);

        assertThat(names.queueNameOrUnknown(0)).isEqualTo("unknown");
        assertThat(names.typeNameOrUnknown(0)).isEqualTo("unknown");

        assertThat(store.queueNamesCalls).hasValue(0);
        assertThat(store.typeNamesCalls).hasValue(0);
    }

    @Test
    void anEventLoadsAnIdAnotherProcessRegisteredFromTheRegistry() {
        var store = new RegistryStore();
        int foreignQueue = store.queueId("Q:OTHER");
        int foreignType = store.typeId("other-type");
        var names = new Names(store);

        assertThat(names.queueNameOrUnknown(foreignQueue)).isEqualTo("Q:OTHER");
        assertThat(names.typeNameOrUnknown(foreignType)).isEqualTo("other-type");
    }

    @Test
    void viewResolvesQueueAndTypeNames() {
        var store = new RegistryStore();
        var names = new Names(store);
        var record = TaskRecord.builder()
                .id(5)
                .queueId(names.queueId("Q:HIGH"))
                .typeId(names.typeId("report"))
                .state(TaskState.QUEUED)
                .build();

        var view = names.view(record);

        assertThat(view.id()).isEqualTo(5);
        assertThat(view.queue()).isEqualTo("Q:HIGH");
        assertThat(view.type()).isEqualTo("report");
        assertThat(view.state()).isEqualTo(TaskState.QUEUED);
    }
}
