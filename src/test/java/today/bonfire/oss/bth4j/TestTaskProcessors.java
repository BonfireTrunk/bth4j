package today.bonfire.oss.bth4j;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.service.TaskProcessor;
import today.bonfire.oss.bth4j.service.TaskProcessorRegistry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TestTaskProcessors {
    // Counters for verification
    public static final AtomicInteger regularTaskExecutions = new AtomicInteger(0);
    public static final AtomicInteger delayedTaskExecutions = new AtomicInteger(0);
    public static final AtomicInteger recurringTaskExecutions = new AtomicInteger(0);
    public static final AtomicInteger retryTaskExecutions = new AtomicInteger(0);
    public static final AtomicInteger concurrentTaskExecutions = new AtomicInteger(0);
    public static CountDownLatch completionLatch;
    public static CountDownLatch concurrencyLatch;

    public static void resetCounters() {
        regularTaskExecutions.set(0);
        delayedTaskExecutions.set(0);
        recurringTaskExecutions.set(0);
        retryTaskExecutions.set(0);
        concurrentTaskExecutions.set(0);
    }

    public static TaskProcessorRegistry createTestRegistry() {
        TaskProcessorRegistry registry = new TaskProcessorRegistry();
        registry.register(TestEvents.REGULAR_TASK, new RegularTaskProcessor());
        registry.register(TestEvents.DELAYED_TASK, new DelayedTaskProcessor());
        registry.register(TestEvents.RECURRING_TASK, new RecurringTaskProcessor());
        registry.register(TestEvents.RETRY_TASK, new RetryTaskProcessor());
        registry.register(TestEvents.CONCURRENT_TASK, new ConcurrentTaskProcessor());
        return registry;
    }

    private static class RegularTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            log.info("Processing regular task: {}", task.taskString());
            regularTaskExecutions.incrementAndGet();
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private static class DelayedTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            log.info("Processing delayed task: {}", task.taskString());
            delayedTaskExecutions.incrementAndGet();
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private static class RecurringTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            log.info("Processing recurring task: {}", task.taskString());
            recurringTaskExecutions.incrementAndGet();
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private static class RetryTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            log.info("Processing retry task: {}", task.taskString());
            int executions = retryTaskExecutions.incrementAndGet();
            if (executions <= 2) { // Fail first two times
                throw new RuntimeException("Simulated failure for retry testing");
            }
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private static class ConcurrentTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            try {
                log.info("Processing concurrent task: {}", task.taskString());
                int current = concurrentTaskExecutions.incrementAndGet();
                if (concurrencyLatch != null) {
                    // Wait for other tasks to start
                    concurrencyLatch.await();
                }
                Thread.sleep(100); // Small delay to ensure overlap
                concurrentTaskExecutions.decrementAndGet();
                if (completionLatch != null) {
                    completionLatch.countDown();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
