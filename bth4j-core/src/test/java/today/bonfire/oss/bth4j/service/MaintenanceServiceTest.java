package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.store.ExpiredLease;
import today.bonfire.oss.bth4j.store.FailOutcome;
import today.bonfire.oss.bth4j.store.TaskStore;

class MaintenanceServiceTest {

    private static MaintenanceService service(TaskStore store) {
        var ops = new TaskOps(
                store,
                null,
                TestEvents.UNKNOWN::from,
                new QueuesHolder(Set.of("def"), List.of("def"), "def"),
                Duration.ofDays(1));
        return new MaintenanceService.Builder()
                .setGroup(new ThreadGroup("MaintenanceServiceTest"))
                .setThreadName("maintenance")
                .setTaskOps(ops)
                .setCheckInterval(Duration.ofMillis(20))
                .setDeadRetention(Duration.ofDays(1))
                .setTaskRetryDelay(attempts -> Duration.ofSeconds(5))
                .create();
    }

    @Test
    void expiredLeasesAreFailedOnlyWhileStillExpired() throws InterruptedException {
        var store = mock(TaskStore.class);
        when(store.reclaim(anyInt()))
                .thenReturn(List.of(new ExpiredLease("a", "t1", 1, 3, TestEvents.REGULAR_TASK.value())))
                .thenReturn(List.of());
        when(store.failExpired(anyString(), anyString(), any(), anyString(), any()))
                .thenReturn(FailOutcome.RETRY);
        var service = service(store);

        service.start();
        try {
            verify(store, timeout(5000))
                    .failExpired("a", "t1", Duration.ofSeconds(5), "lease expired after attempt 1", Duration.ofDays(1));
            verify(store, never()).fail(anyString(), anyString(), any(), anyString(), any());
        } finally {
            service.stopThread();
        }
        assertThat(service.waitTillDone(Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void stopReturnsAtOnceAndWaitTillDoneWaitsForTheScheduler() throws InterruptedException {
        var store = mock(TaskStore.class);
        when(store.reclaim(anyInt())).thenReturn(List.of());
        var service = service(store);
        service.start();
        verify(store, timeout(5000)).reclaim(anyInt());

        long start = System.nanoTime();
        service.stopThread();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));

        assertThat(service.waitTillDone(Duration.ofSeconds(5))).isTrue();
        assertThat(service.isRunning()).isFalse();
    }
}
