package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.store.TaskRecord;
import today.bonfire.oss.bth4j.store.TaskStore;

/** Only what needs no Redis; the transitions are covered by {@link TaskOpsIT}. */
class TaskOpsTest {

    private static TaskOps opsWithoutRedis() {
        return new TaskOps(
                mock(TaskStore.class),
                null,
                TestEvents.UNKNOWN::from,
                new QueuesHolder(Set.of("def"), List.of("def"), "def"),
                Duration.ofDays(1));
    }

    @Test
    void dataForBlankIdIsNull() {
        var ops = opsWithoutRedis();

        assertThat(ops.getDataForTask(null, String.class)).isNull();
        assertThat(ops.getDataForTask("", String.class)).isNull();
        assertThat(ops.getDataForTask("   ", String.class)).isNull();
    }

    @Test
    void decodeDataOfTaskWithoutDataIsNull() {
        var task = Task.from(TaskRecord.newTask("id", "3", "1", null, null, 0, null), TestEvents.UNKNOWN::from);

        assertThat(opsWithoutRedis().decodeData(task, String.class)).isNull();
    }
}
