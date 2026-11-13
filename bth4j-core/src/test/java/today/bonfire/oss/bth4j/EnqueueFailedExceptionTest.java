package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.EnqueueFailedException;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

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
        var failure = new EnqueueFailedException(ids, List.of(), notStored, new TaskStoreException("failed"));
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
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void emptyIdsReturnEveryRequestUnchangedIncludingCallerIds() {
        var requests =
                List.of(TaskRequest.of(VOID).build(), TaskRequest.of(VOID).id(7).build());
        var failure = new EnqueueFailedException(List.of(), List.of(), Set.of(0), new TaskStoreException("failed"));

        var retry = failure.retryRequests(requests);

        assertThat(retry.get(0)).isSameAs(requests.get(0));
        assertThat(retry.get(1)).isSameAs(requests.get(1));
    }

    @Test
    void existingConstructorKeepsAllIdsForRetry() {
        var failure = new EnqueueFailedException(List.of(7L), List.of(), new TaskStoreException("failed"));

        assertThat(failure.notStored()).isEmpty();
        assertThat(failure.retryRequests(List.of(TaskRequest.of(VOID).build()))
                        .getFirst()
                        .id())
                .isEqualTo(7L);
    }

    @Test
    void retryRequestsLeaveOutTheSettledRequestsSoASettledDuplicateIsNotSentAgain() {
        var requests = List.of(
                TaskRequest.of(VOID).uniqueKey("k").build(),
                TaskRequest.of(VOID).build(),
                TaskRequest.of(VOID).build());
        List<EnqueueResult> settled = List.of(new EnqueueResult.Duplicate(99L), new EnqueueResult.Accepted(20L, true));
        var failure =
                new EnqueueFailedException(List.of(10L, 20L, 30L), settled, Set.of(), new TaskStoreException("failed"));

        var retry = failure.retryRequests(requests);

        assertThat(failure.settled()).containsExactlyElementsOf(settled);
        assertThat(failure.storedCount()).isEqualTo(2);
        assertThat(retry).hasSize(1);
        assertThat(retry.getFirst().id()).isEqualTo(30L);
        assertThatThrownBy(() ->
                        new EnqueueFailedException(List.of(10L), settled, Set.of(), new TaskStoreException("failed")))
                .isInstanceOf(TaskValidationException.class);
    }
}
