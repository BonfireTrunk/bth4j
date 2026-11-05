package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

class ConcurrencyTest {

    @Test
    void fixedAcceptsTheBounds() {
        assertThat(Concurrency.fixed(1)).isEqualTo(new Concurrency.Fixed(1));
        assertThat(Concurrency.fixed(100_000)).isEqualTo(new Concurrency.Fixed(100_000));
    }

    @Test
    void fixedRejectsOutOfRangeLimits() {
        assertThatThrownBy(() -> Concurrency.fixed(0)).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> Concurrency.fixed(-1)).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> Concurrency.fixed(100_001)).isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void adaptiveAcceptsMinEqualToMax() {
        var adaptive = Concurrency.adaptive(2, 2);

        assertThat(adaptive).isEqualTo(new Concurrency.Adaptive(2, 2));
    }

    @Test
    void adaptiveRejectsMinBelowOneAndMaxBelowMin() {
        assertThatThrownBy(() -> Concurrency.adaptive(0, 4)).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> Concurrency.adaptive(5, 4)).isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void defaultIsFixedAtTwiceTheProcessorsAndAtLeastTwo() {
        var expected = Math.max(2, Runtime.getRuntime().availableProcessors() * 2);

        assertThat(Concurrency.defaultConcurrency()).isEqualTo(new Concurrency.Fixed(expected));
    }

    @Test
    void adaptiveDefaultsAndWitherPreserveTargetsAndBounds() {
        var defaults = Concurrency.adaptive(2, 20);
        assertThat(defaults.cpuTarget()).isEqualTo(0.90);
        assertThat(defaults.memoryTarget()).isEqualTo(0.80);
        assertThat(defaults.memorySignal()).isEqualTo(MemorySignal.HEAP);
        var custom = Concurrency.adaptive(3, 30, 0.6, 0.7).withMemorySignal(MemorySignal.HEAP_AND_SYSTEM);
        assertThat(custom).isEqualTo(new Concurrency.Adaptive(3, 30, 0.6, 0.7, MemorySignal.HEAP_AND_SYSTEM));
        assertThat(defaults.memorySignal()).isEqualTo(MemorySignal.HEAP);
    }

    @Test
    void adaptiveValidationExplainsEachInvalidSetting() {
        assertThatThrownBy(() -> Concurrency.adaptive(0, 4)).hasMessageContaining("1 <= min <= max");
        assertThatThrownBy(() -> Concurrency.adaptive(5, 4)).hasMessageContaining("1 <= min <= max");
        for (double invalid :
                new double[] {0, -0.1, 0.951, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThatThrownBy(() -> Concurrency.adaptive(1, 4, invalid, 0.8))
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("0 < cpuTarget <= 0.95");
        }
        for (double invalid :
                new double[] {0, -0.1, 0.901, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThatThrownBy(() -> Concurrency.adaptive(1, 4, 0.9, invalid))
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("0 < memoryTarget <= 0.90");
        }
        assertThatThrownBy(() -> Concurrency.adaptive(1, 4).withMemorySignal(null))
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageContaining("memorySignal must not be null");
        assertThat(Concurrency.adaptive(1, 1, 0.95, 0.90).cpuTarget()).isEqualTo(0.95);
    }
}
