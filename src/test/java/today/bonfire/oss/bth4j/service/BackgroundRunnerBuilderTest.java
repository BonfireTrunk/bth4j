package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

/**
 * Tests of the builder and queue setup. None of these need Redis: the builder only reads its own state
 * and every failing {@code build()} call fails before a Jedis client would be needed.
 */
class BackgroundRunnerBuilderTest {

    // defaults

    @Test
    void defaultRetryDelayIsTheCubeOfTheRetryCount() {
        var delay = new BackgroundRunner.Builder().getTaskRetryDelay();
        assertThat(delay.apply(0)).isEqualTo(0);
        assertThat(delay.apply(1)).isEqualTo(1);
        assertThat(delay.apply(2)).isEqualTo(8);
        assertThat(delay.apply(3)).isEqualTo(27);
        assertThat(delay.apply(4)).isEqualTo(64);
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
    void buildRequiresJedisClient() {
        assertThatThrownBy(() -> new BackgroundRunner.Builder()
                        .eventParser(TestEvents.UNKNOWN::from)
                        .jsonMapper(new JsonMapperTest())
                        .build())
                .isInstanceOf(TaskConfigurationError.class)
                .hasMessageContaining("jedis");
    }

    // invalid settings

    @Test
    void durationsMustBePositive() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.staleTaskTimeout(null)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.staleTaskTimeout(Duration.ZERO)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.staleTaskTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.BGThreadCheckInterval(Duration.ZERO))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.taskProcessorQueueCheckInterval(Duration.ofMillis(-5)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.recurringTasksQueueCheckInterval(null))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.setRecurringTasksQueueAheadDuration(Duration.ZERO))
                .isInstanceOf(TaskConfigurationError.class);
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
    void delayedTasksQueueCheckIntervalHasTenMillisecondMinimum() {
        var builder = new BackgroundRunner.Builder();
        assertThatThrownBy(() -> builder.delayedTasksQueueCheckInterval(Duration.ofMillis(9)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThat(builder.delayedTasksQueueCheckInterval(Duration.ofMillis(10)))
                .isSameAs(builder);
    }

    @Test
    void cronLockDurationIsChainableAndValidated() {
        var builder = new BackgroundRunner.Builder();
        assertThat(builder.cronLockDuration(Duration.ofMinutes(1))).isSameAs(builder);
        assertThatThrownBy(() -> builder.cronLockDuration(Duration.ofMillis(500)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.cronLockDuration(Duration.ZERO)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.cronLockDuration(null)).isInstanceOf(TaskConfigurationError.class);
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
        assertThatThrownBy(() -> builder.namespacePrefix(" ")).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.threadGroupName("")).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void customRetryDelayReplacesTheDefault() {
        var builder = new BackgroundRunner.Builder().taskRetryDelay(count -> 5);
        assertThat(builder.getTaskRetryDelay().apply(3)).isEqualTo(5);
    }

    // setupQueues

    @Test
    void setupQueuesPrefixesNamespaceAndKeepsOrder() {
        QueuesHolder holder = BackgroundRunner.setupQueues("NS", Set.of("Q:A", "Q:B"), List.of("Q:B", "Q:A"), "Q:A");

        assertThat(holder.namespace).isEqualTo("NS");
        assertThat(holder.defaultQueue).isEqualTo("NS:Q:A");
        assertThat(holder.availableQueues).containsExactlyInAnyOrder("NS:Q:A", "NS:Q:B");
        assertThat(holder.queuesToProcess).containsExactly("NS:Q:B", "NS:Q:A");
    }

    @Test
    void setupQueuesResolvesValidQueuesAndRejectsUnknownOnes() {
        var holder = BackgroundRunner.setupQueues("NS", Set.of("Q:A", "Q:B"), List.of("Q:A"), "Q:A");

        assertThat(holder.getValidQueue("Q:B")).isEqualTo("NS:Q:B");
        assertThat(holder.getValidQueue(null)).isEqualTo("NS:Q:A");
        assertThat(holder.getValidQueue(" ")).isEqualTo("NS:Q:A");
        assertThatThrownBy(() -> holder.getValidQueue("Q:Z")).isInstanceOf(TaskErrorException.class);
    }

    @Test
    void setupQueuesRejectsQueueToProcessThatIsNotAvailable() {
        assertThatThrownBy(() -> BackgroundRunner.setupQueues("NS", Set.of("Q:A"), List.of("Q:B"), "Q:A"))
                .isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void setupQueuesRejectsDefaultQueueThatIsNotAvailable() {
        assertThatThrownBy(() -> BackgroundRunner.setupQueues("NS", Set.of("Q:A"), List.of("Q:A"), "Q:B"))
                .isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void setupQueuesRejectsEmptyQueuesToProcess() {
        assertThatThrownBy(() -> BackgroundRunner.setupQueues("NS", Set.of("Q:A"), List.of(), "Q:A"))
                .isInstanceOf(TaskConfigurationError.class);
    }
}
