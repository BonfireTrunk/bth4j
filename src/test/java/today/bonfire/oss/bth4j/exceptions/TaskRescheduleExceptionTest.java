package today.bonfire.oss.bth4j.exceptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TaskRescheduleExceptionTest {

    @Test
    void delayIsKept() {
        assertThat(new TaskRescheduleException(45).delay()).isEqualTo(45);
        assertThat(new TaskRescheduleException(0).delay()).isZero();
    }

    @Test
    void delayIsKeptWithMessageAndCause() {
        var cause = new IllegalStateException("upstream busy");
        var ex = new TaskRescheduleException(60, "try later", cause);
        assertThat(ex.delay()).isEqualTo(60);
        assertThat(ex).hasMessage("try later").hasCause(cause);
    }

    @Test
    void constructorWithoutDelayMeansNoDelay() {
        var ex = new TaskRescheduleException("try again", new RuntimeException("x"));
        assertThat(ex.delay()).isZero();
    }

    @Test
    void negativeDelayIsRejected() {
        assertThatThrownBy(() -> new TaskRescheduleException(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TaskRescheduleException(-5, "m", null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
