package today.bonfire.oss.bth4j.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;

class InMemoryStoreBuilderTest {

    @Test
    void theClockDefaultsToTheSystemClock() {
        var before = Instant.now();
        var store = InMemoryStore.builder().build();
        store.allocateRunnerId("r", Set.of(), Set.of());
        var after = Instant.now();

        assertThat(store.runners().getFirst().startedAt()).isBetween(before.minusSeconds(1), after.plusSeconds(1));
    }

    @Test
    void theStoreTakesItsTimeFromTheClockOnTheBuilder() {
        var clock = new MutableClock(Instant.parse("2030-01-01T00:00:00Z"));
        var store = InMemoryStore.builder().clock(clock).build();

        store.allocateRunnerId("r", Set.of(), Set.of());

        assertThat(store.runners().getFirst().startedAt()).isEqualTo(Instant.parse("2030-01-01T00:00:00Z"));
    }

    @Test
    void theRegistryBoundCapsTheNumberOfQueueNames() {
        var store = InMemoryStore.builder().registryBound(2).build();
        store.queueId("a");
        store.queueId("b");

        assertThatThrownBy(() -> store.queueId("c"))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("the limit is 2");
    }

    @Test
    void oneBuilderBuildsTwoIndependentStores() {
        var clock = new MutableClock();
        var builder = InMemoryStore.builder().clock(clock);
        var first = builder.build();
        var second = builder.clock(new MutableClock(clock.instant().plus(Duration.ofDays(1))))
                .build();

        first.allocateRunnerId("r", Set.of(), Set.of());

        assertThat(second.runners()).isEmpty();
        assertThat(first.runners()).hasSize(1);
    }
}
