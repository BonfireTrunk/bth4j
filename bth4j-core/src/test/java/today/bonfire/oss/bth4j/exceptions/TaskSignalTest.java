package today.bonfire.oss.bth4j.exceptions;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class TaskSignalTest {

    @Test
    void theFourControlFlowExceptionsAreTheOnlySignals() {
        assertThat(TaskSignal.class.isSealed()).isTrue();
        assertThat(List.of(TaskSignal.class.getPermittedSubclasses()))
                .containsExactlyInAnyOrder(
                        TaskRescheduleException.class,
                        TaskUnrecoverableException.class,
                        TaskLeaseLostException.class,
                        TaskCancelledException.class);
    }

    @Test
    void oneCatchLetsEverySignalThroughAWrappingHandler() {
        List<RuntimeException> signals = List.of(
                new TaskRescheduleException(Duration.ofSeconds(1)),
                new TaskUnrecoverableException("dead"),
                new TaskLeaseLostException("gone"),
                new TaskCancelledException("stop"));
        for (var signal : signals) {
            RuntimeException passed;
            try {
                try {
                    throw signal;
                } catch (TaskSignal s) {
                    throw s;
                } catch (RuntimeException e) {
                    throw new IllegalStateException("wrapped");
                }
            } catch (RuntimeException e) {
                passed = e;
            }
            assertThat(passed).isSameAs(signal);
        }
    }
}
