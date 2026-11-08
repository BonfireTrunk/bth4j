package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.testing.InMemoryStore;

class RunnerConcurrencyTest {
    @Test
    void fixedLimitIsAvailableBeforeStart() {
        var runner = BackgroundRunner.builder()
                .store(new InMemoryStore())
                .concurrency(Concurrency.fixed(7))
                .build();
        try {
            assertThat(runner.concurrencyLimit()).hasValue(7);
        } finally {
            runner.stop();
        }
    }

    @Test
    void adaptiveExecutorReceivesAllRecordSettings() throws Exception {
        var settings = Concurrency.adaptive(3, 37, 0.6, 0.7).withMemorySignal(MemorySignal.HEAP_AND_SYSTEM);
        var runner = BackgroundRunner.builder()
                .store(new InMemoryStore())
                .concurrency(settings)
                .build();
        try {
            assertThat(runner.concurrencyLimit()).hasValue(3);
            var executor = field(runner, "taskExecutor");
            assertThat(field(field(executor, "controller"), "settings")).isEqualTo(settings);
            assertThat(field(field(executor, "sampler"), "signal")).isEqualTo(MemorySignal.HEAP_AND_SYSTEM);
        } finally {
            runner.stop();
        }
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
