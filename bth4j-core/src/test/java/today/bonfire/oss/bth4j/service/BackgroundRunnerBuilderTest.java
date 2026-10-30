package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.JsonMapperTest;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.store.TaskStore;

/**
 * Tests of the builder and queue setup. None of these need a running Redis: the client is created
 * without connecting and the runners built here are never started.
 */
class BackgroundRunnerBuilderTest {

    private static final TaskStore STORE = mock(TaskStore.class);

    // defaults

    @Test
    void defaultRetryDelayGrowsExponentiallyWithJitter() {
        for (int i = 0; i < 50; i++) {
            assertThat(BackgroundRunner.defaultRetryDelay(1))
                    .isBetween(Duration.ofMillis(800), Duration.ofMillis(1200));
            assertThat(BackgroundRunner.defaultRetryDelay(3))
                    .isBetween(Duration.ofMillis(3200), Duration.ofMillis(4800));
            assertThat(BackgroundRunner.defaultRetryDelay(100)).isLessThanOrEqualTo(Duration.ofMinutes(72));
        }
    }

    @Test
    void builderUsesDefaultRetryDelayUnlessReplaced() {
        var delay = new BackgroundRunner.Builder().getTaskRetryDelay().apply(1);
        assertThat(delay).isBetween(Duration.ofMillis(800), Duration.ofMillis(1200));
    }

    @Test
    void defaultQueuesAreOrderedAndDefaultQueueIsDeterministic() {
        var builder = new BackgroundRunner.Builder();
        assertThat(builder.getDefaultQueue()).isEqualTo("Q:DEF");
        assertThat(builder.getQueuesToProcess()).containsExactly("Q:DEF", "Q:LOW", "Q:MED", "Q:HIGH");
        assertThat(builder.getAvailableQueues()).containsExactly("Q:DEF", "Q:LOW", "Q:MED", "Q:HIGH");
    }

    @Test
    void configureQueuesKeepsTheOrderOfTheCaller() {
        var builder = new BackgroundRunner.Builder().configureQueues(List.of("z", "y", "x"), null, null);
        assertThat(builder.getAvailableQueues()).containsExactly("z", "y", "x");
        assertThat(builder.getQueuesToProcess()).containsExactly("z", "y", "x");
        assertThat(builder.getDefaultQueue()).isEqualTo("z");
    }

    @Test
    void configureQueuesUsesGivenQueuesToProcessAndDefault() {
        var builder =
                new BackgroundRunner.Builder().configureQueues(List.of("a", "b", "c"), List.of("c", "b", "b"), "a");
        assertThat(builder.getQueuesToProcess()).containsExactly("c", "b", "b");
        assertThat(builder.getDefaultQueue()).isEqualTo("a");
    }

    @Test
    void configureQueuesRemovesDuplicatesFromAvailableQueues() {
        var builder = new BackgroundRunner.Builder().configureQueues(List.of("a", "b", "a"), List.of(), " ");
        assertThat(builder.getAvailableQueues()).containsExactly("a", "b");
        assertThat(builder.getQueuesToProcess()).containsExactly("a", "b");
        assertThat(builder.getDefaultQueue()).isEqualTo("a");
    }

    @Test
    void configureQueuesRejectsEmptyOrBlankQueues() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.configureQueues(null, null, null)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.configureQueues(List.of(), null, null))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.configureQueues(List.of("a", " "), null, null))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.configureQueues(Arrays.asList("a", null), null, null))
                .isInstanceOf(TaskConfigurationError.class);
    }

    // required inputs

    @Test
    void buildRequiresEventParser() {
        assertThatThrownBy(() -> new BackgroundRunner.Builder().build())
                .isInstanceOf(TaskConfigurationError.class)
                .hasMessageContaining("event parser");
    }

    @Test
    void buildRequiresTaskStore() {
        assertThatThrownBy(() -> new BackgroundRunner.Builder()
                        .eventParser(TestEvents.UNKNOWN::from)
                        .jsonMapper(new JsonMapperTest())
                        .build())
                .isInstanceOf(TaskConfigurationError.class)
                .hasMessageContaining("task store");
    }

    @Test
    void buildingTwiceGivesRunnersWithDifferentOps() {
        var builder = new BackgroundRunner.Builder()
                .eventParser(TestEvents.UNKNOWN::from)
                .jsonMapper(new JsonMapperTest())
                .taskStore(STORE);

        var first = builder.build();
        var second = builder.build();

        assertThat(first).isNotSameAs(second);
        assertThat(first.taskOps()).isNotSameAs(second.taskOps());
    }

    // invalid settings

    @Test
    void durationsMustBePositive() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.leaseDuration(null)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.leaseDuration(Duration.ZERO)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.leaseDuration(Duration.ofSeconds(-1)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.idlePollInterval(Duration.ZERO)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.idlePollInterval(Duration.ofMillis(-5)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.maintenanceCheckInterval(Duration.ZERO))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.scheduledTasksCheckInterval(Duration.ZERO))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.scheduledTasksCheckInterval(null)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.recurringTasksCheckInterval(null)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.recurringTasksCheckInterval(Duration.ofSeconds(-1)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.recurringTasksQueueAheadDuration(Duration.ZERO))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.recurringTasksQueueAheadDuration(null))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.deadTaskRetention(Duration.ZERO)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.deadTaskRetention(Duration.ofDays(-1)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.deadTaskRetention(null)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.serviceCheckInterval(Duration.ZERO))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.serviceCheckInterval(null)).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void leaseDurationHasOneSecondMinimum() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.leaseDuration(Duration.ofMillis(999)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThat(builder.leaseDuration(Duration.ofSeconds(1))).isSameAs(builder);
    }

    @Test
    void idlePollIntervalHasTenMillisecondMinimum() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.idlePollInterval(Duration.ofMillis(9)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThat(builder.idlePollInterval(Duration.ofMillis(10))).isSameAs(builder);
    }

    @Test
    void maintenanceCheckIntervalHasOneSecondMinimum() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.maintenanceCheckInterval(Duration.ofMillis(999)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.maintenanceCheckInterval(null)).isInstanceOf(TaskConfigurationError.class);
        assertThat(builder.maintenanceCheckInterval(Duration.ofSeconds(1))).isSameAs(builder);
    }

    @Test
    void scheduledTasksCheckIntervalHasTenMillisecondMinimum() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.scheduledTasksCheckInterval(Duration.ofMillis(9)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThat(builder.scheduledTasksCheckInterval(Duration.ofMillis(10))).isSameAs(builder);
    }

    @Test
    void claimBatchSizeMustBeAtLeastOne() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.claimBatchSize(0)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.claimBatchSize(-3)).isInstanceOf(TaskConfigurationError.class);
        assertThat(builder.claimBatchSize(1)).isSameAs(builder);
    }

    @Test
    void shutdownTimeoutMayBeZeroButNotNegative() {
        var builder = new BackgroundRunner.Builder();
        assertThat(builder.shutdownTimeout(Duration.ZERO)).isSameAs(builder);
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.shutdownTimeout(null)).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void nullRetryDelayAndBlankNamesAreRejected() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.taskRetryDelay(null)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.threadGroupName("")).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void customRetryDelayReplacesTheDefault() {
        var builder = new BackgroundRunner.Builder().taskRetryDelay(count -> Duration.ofSeconds(5));
        assertThat(builder.getTaskRetryDelay().apply(3)).isEqualTo(Duration.ofSeconds(5));
    }

    // setupQueues

    @Test
    void setupQueuesKeepsLogicalNamesAndOrder() {
        QueuesHolder holder = BackgroundRunner.setupQueues(Set.of("Q:A", "Q:B"), List.of("Q:B", "Q:A"), "Q:A");

        assertThat(holder.defaultQueue).isEqualTo("Q:A");
        assertThat(holder.availableQueues).containsExactlyInAnyOrder("Q:A", "Q:B");
        assertThat(holder.queuesToProcess).containsExactly("Q:B", "Q:A");
    }

    @Test
    void setupQueuesResolvesValidQueuesAndRejectsUnknownOnes() {
        var holder = BackgroundRunner.setupQueues(Set.of("Q:A", "Q:B"), List.of("Q:A"), "Q:A");

        assertThat(holder.getValidQueue("Q:B")).isEqualTo("Q:B");
        assertThat(holder.getValidQueue(null)).isEqualTo("Q:A");
        assertThat(holder.getValidQueue(" ")).isEqualTo("Q:A");
        assertThatThrownBy(() -> holder.getValidQueue("Q:Z")).isInstanceOf(TaskErrorException.class);
    }

    @Test
    void setupQueuesRejectsQueueToProcessThatIsNotAvailable() {
        assertThatThrownBy(() -> BackgroundRunner.setupQueues(Set.of("Q:A"), List.of("Q:B"), "Q:A"))
                .isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void setupQueuesRejectsDefaultQueueThatIsNotAvailable() {
        assertThatThrownBy(() -> BackgroundRunner.setupQueues(Set.of("Q:A"), List.of("Q:A"), "Q:B"))
                .isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void setupQueuesRejectsEmptyQueuesToProcess() {
        assertThatThrownBy(() -> BackgroundRunner.setupQueues(Set.of("Q:A"), List.of(), "Q:A"))
                .isInstanceOf(TaskConfigurationError.class);
    }
}
