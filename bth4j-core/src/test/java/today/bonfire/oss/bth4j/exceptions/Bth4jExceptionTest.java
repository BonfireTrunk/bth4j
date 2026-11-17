package today.bonfire.oss.bth4j.exceptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class Bth4jExceptionTest {

    @Test
    void oneCatchCoversEverythingBth4jRefusesOrReports() {
        List<RuntimeException> thrown = List.of(
                new TaskValidationException("bad value"),
                new TaskConfigurationException("bad setting"),
                new TaskStoreException("store down"),
                new UnreadableTaskException(1, "task 1 has a stored value that cannot be read", null),
                new TaskIdReusedException(1, "another payload"),
                new EnqueueFailedException(List.of(), List.of(), new TaskStoreException("store down")),
                new TaskDataException("cannot be converted"));

        assertThat(thrown).allSatisfy(e -> assertThat(e).isInstanceOf(Bth4jException.class));
    }

    @Test
    void theSignalsOfAProcessorStayOutsideSoThatTheCatchCannotSwallowThem() {
        assertThat(Bth4jException.class.isAssignableFrom(TaskSignal.class)).isFalse();
        assertThat(new TaskRescheduleException(Duration.ofSeconds(1))).isNotInstanceOf(Bth4jException.class);
    }

    @Test
    void anUnreadableTaskIsNotAStoreFailure() {
        assertThat(new UnreadableTaskException(1, "task 1 has a stored value that cannot be read", null))
                .isNotInstanceOf(TaskStoreException.class);
    }

    @Test
    void aConfigurationMessageIsTheTextItWasGiven() {
        assertThat(new TaskConfigurationException("limit must be at least 1")).hasMessage("limit must be at least 1");
        assertThat(new TaskConfigurationException("limit must be at least 1", new RuntimeException()))
                .hasMessage("limit must be at least 1");
    }

    @Test
    void aRescheduleDelayThatIsMissingOrNegativeIsRefused() {
        assertThatThrownBy(() -> new TaskRescheduleException(null)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> new TaskRescheduleException(Duration.ofMillis(-1)))
                .isInstanceOf(TaskValidationException.class);
    }
}
