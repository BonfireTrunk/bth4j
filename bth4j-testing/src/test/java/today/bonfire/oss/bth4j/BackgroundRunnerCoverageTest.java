package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

class BackgroundRunnerCoverageTest {
    @Test
    void registersOnlyProcessedQueuesAndRegisteredTypesBeforeServicesStart() {
        var clock = new MutableClock();
        var store = new InMemoryStore(clock);
        store.typeId("unregistered");
        var runner = BackgroundRunner.builder()
                .store(store)
                .clock(clock)
                .configureQueues(List.of("main", "mail", "unused"), List.of("mail", "main", "mail"), "main")
                .workerLabel("coverage-test")
                .build();
        runner.registry().register(TaskType.of("send"), (context, payload) -> {});
        runner.registry().register(TaskType.of("export"), (context, payload) -> {});
        try {
            runner.start();

            assertThat(store.workers()).singleElement().satisfies(w -> {
                assertThat(w.id()).isEqualTo(runner.workerId());
                assertThat(w.label()).isEqualTo("coverage-test");
                assertThat(w.queueIds()).containsExactlyInAnyOrder(store.queueId("main"), store.queueId("mail"));
                assertThat(w.typeIds()).containsExactlyInAnyOrder(store.typeId("send"), store.typeId("export"));
            });
        } finally {
            runner.stop();
        }
    }
}
