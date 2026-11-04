package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.EnqueueFailedException;
import today.bonfire.oss.bth4j.spi.TaskStoreException;

class EnqueueFailedExceptionTest {
    private static final TaskType<Void> VOID = TaskType.of("retry");

    @Test
    void retryRequestsPreservesUnwrittenRequestsAndAssignsOtherIdsWithoutMutatingTheInput() {
        var requests = List.of(
                TaskRequest.of(VOID).build(),
                TaskRequest.of(VOID).build(),
                TaskRequest.of(VOID).id(30).build());
        var ids = new ArrayList<>(List.of(10L, 20L, 30L));
        var notStored = new HashSet<>(Set.of(1));
        var failure = new EnqueueFailedException(ids, 0, notStored, new TaskStoreException("failed"));
        ids.clear();
        notStored.clear();

        var retry = failure.retryRequests(requests);

        assertThat(failure.ids()).containsExactly(10L, 20L, 30L);
        assertThat(failure.notStored()).containsExactly(1);
        assertThat(retry.get(0).id()).isEqualTo(10L);
        assertThat(retry.get(1)).isSameAs(requests.get(1));
        assertThat(retry.get(2).id()).isEqualTo(30L);
        assertThat(requests.get(0).id()).isNull();
        assertThatThrownBy(() -> failure.notStored().add(0)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> retry.add(requests.get(0))).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> failure.retryRequests(requests.subList(0, 2)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyIdsReturnEveryRequestUnchangedIncludingCallerIds() {
        var requests =
                List.of(TaskRequest.of(VOID).build(), TaskRequest.of(VOID).id(7).build());
        var failure = new EnqueueFailedException(List.of(), 0, Set.of(0), new TaskStoreException("failed"));

        var retry = failure.retryRequests(requests);

        assertThat(retry.get(0)).isSameAs(requests.get(0));
        assertThat(retry.get(1)).isSameAs(requests.get(1));
    }

    @Test
    void existingConstructorKeepsAllIdsForRetry() {
        var failure = new EnqueueFailedException(List.of(7L), 0, new TaskStoreException("failed"));

        assertThat(failure.notStored()).isEmpty();
        assertThat(failure.retryRequests(List.of(TaskRequest.of(VOID).build()))
                        .getFirst()
                        .id())
                .isEqualTo(7L);
    }
}
