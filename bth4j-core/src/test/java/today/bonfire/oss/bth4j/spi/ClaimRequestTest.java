package today.bonfire.oss.bth4j.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

class ClaimRequestTest {
    @Test
    void rejectsInvalidSharesCapsAndDuplicateOrEmptyGrants() {
        assertThatThrownBy(() -> new QueueGrant(1, -1, 2)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> new QueueGrant(1, 0, -1)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> new QueueGrant(1, 3, 2)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> request(List.of())).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> request(List.of(new QueueGrant(1, 1, 1), new QueueGrant(1, 0, 2))))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void snapshotsGrantsAndAcceptsZeroShares() {
        var grants = new ArrayList<>(List.of(new QueueGrant(1, 0, 0), new QueueGrant(2, 0, 2)));
        var request = request(grants);
        grants.clear();
        assertThat(request.grants()).containsExactly(new QueueGrant(1, 0, 0), new QueueGrant(2, 0, 2));
    }

    private ClaimRequest request(List<QueueGrant> grants) {
        return new ClaimRequest(grants, 2, Duration.ofSeconds(30), 1, Set.of(1));
    }
}
