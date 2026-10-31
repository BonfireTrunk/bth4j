package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

class ConcurrencyTest {

    @Test
    void fixedAcceptsTheBounds() {
        assertThat(Concurrency.fixed(1)).isEqualTo(new Concurrency.Fixed(1));
        assertThat(Concurrency.fixed(100_000)).isEqualTo(new Concurrency.Fixed(100_000));
    }

    @Test
    void fixedRejectsOutOfRangeLimits() {
        assertThatThrownBy(() -> Concurrency.fixed(0)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> Concurrency.fixed(-1)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> Concurrency.fixed(100_001)).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void adaptiveAcceptsMinEqualToMax() {
        var adaptive = Concurrency.adaptive(2, 2);

        assertThat(adaptive).isEqualTo(new Concurrency.Adaptive(2, 2));
    }

    @Test
    void adaptiveRejectsMinBelowOneAndMaxBelowMin() {
        assertThatThrownBy(() -> Concurrency.adaptive(0, 4)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> Concurrency.adaptive(5, 4)).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void defaultIsFixedAtTwiceTheProcessorsAndAtLeastTwo() {
        var expected = Math.max(2, Runtime.getRuntime().availableProcessors() * 2);

        assertThat(Concurrency.defaultConcurrency()).isEqualTo(new Concurrency.Fixed(expected));
    }
}
