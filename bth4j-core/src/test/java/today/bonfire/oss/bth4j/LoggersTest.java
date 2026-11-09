package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

class LoggersTest {

    @Test
    void theDocumentedNamesDoNotChange() {
        assertThat(Loggers.ROOT).isEqualTo("today.bonfire.oss.bth4j");
        assertThat(Loggers.RUNNER).isEqualTo("today.bonfire.oss.bth4j.BackgroundRunner");
        assertThat(Loggers.CLIENT).isEqualTo("today.bonfire.oss.bth4j.TaskClient");
        assertThat(Loggers.PROCESSOR).isEqualTo("today.bonfire.oss.bth4j.TaskProcessorService");
        assertThat(Loggers.ATTEMPT).isEqualTo("today.bonfire.oss.bth4j.TaskExecution");
        assertThat(Loggers.MAINTENANCE).isEqualTo("today.bonfire.oss.bth4j.MaintenanceService");
        assertThat(Loggers.RECURRING).isEqualTo("today.bonfire.oss.bth4j.RecurringTaskService");
        assertThat(Loggers.PROMOTION).isEqualTo("today.bonfire.oss.bth4j.ScheduledTaskService");
        assertThat(Loggers.EXECUTOR).isEqualTo("today.bonfire.oss.bth4j.executor.DefaultVtExecutor");
        assertThat(Loggers.OBSERVERS).isEqualTo("today.bonfire.oss.bth4j.Observers");
        assertThat(Loggers.REDIS_STORE).isEqualTo("today.bonfire.oss.bth4j.redis.RedisTaskStore");
        assertThat(Loggers.TASKS).isEqualTo("bth4j.tasks");
    }

    @Test
    void everyClassWritesToItsDocumentedName() throws Exception {
        var expected = Map.<Class<?>, String>of(
                BackgroundRunner.class, Loggers.RUNNER,
                TaskClient.class, Loggers.CLIENT,
                TaskProcessorService.class, Loggers.PROCESSOR,
                TaskExecution.class, Loggers.ATTEMPT,
                MaintenanceService.class, Loggers.MAINTENANCE,
                RecurringTaskService.class, Loggers.RECURRING,
                ScheduledTaskService.class, Loggers.PROMOTION,
                Observers.class, Loggers.OBSERVERS,
                today.bonfire.oss.bth4j.executor.DefaultVtExecutor.class, Loggers.EXECUTOR);
        for (var e : expected.entrySet()) {
            var field = e.getKey().getDeclaredField("log");
            field.setAccessible(true);
            assertThat(((Logger) field.get(null)).getName())
                    .as(e.getKey().getName())
                    .isEqualTo(e.getValue())
                    .startsWith(Loggers.ROOT + ".");
        }
    }
}
