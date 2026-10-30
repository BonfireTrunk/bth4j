package today.bonfire.oss.bth4j;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.service.Task;
import today.bonfire.oss.bth4j.service.TaskProcessorRegistry;

public class TestTaskProcessors {
    private static final Logger log = LoggerFactory.getLogger(TestTaskProcessors.class);

    // Counters for verification
    public static final AtomicInteger regularTaskExecutions = new AtomicInteger(0);
    public static final AtomicInteger delayedTaskExecutions = new AtomicInteger(0);
    public static final AtomicInteger recurringTaskExecutions = new AtomicInteger(0);
    public static final AtomicInteger retryTaskExecutions = new AtomicInteger(0);
    public static final AtomicInteger concurrentTaskExecutions = new AtomicInteger(0);
    public static final AtomicInteger longTaskExecutions = new AtomicInteger(0);
    public static final AtomicReference<Map<String, Object>> receivedData = new AtomicReference<>();
    public static volatile long longTaskMillis = 3000;
    public static CountDownLatch completionLatch;
    public static CountDownLatch concurrencyLatch;

    public static void resetCounters() {
        regularTaskExecutions.set(0);
        delayedTaskExecutions.set(0);
        recurringTaskExecutions.set(0);
        retryTaskExecutions.set(0);
        concurrentTaskExecutions.set(0);
        longTaskExecutions.set(0);
        receivedData.set(null);
    }

    public static TaskProcessorRegistry createTestRegistry() {
        TaskProcessorRegistry registry = new TaskProcessorRegistry();
        registry.register(TestEvents.REGULAR_TASK, new RegularTaskProcessor());
        registry.register(TestEvents.DELAYED_TASK, new DelayedTaskProcessor());
        registry.register(TestEvents.RECURRING_TASK, new RecurringTaskProcessor());
        registry.register(TestEvents.RETRY_TASK, new RetryTaskProcessor());
        registry.register(TestEvents.CONCURRENT_TASK, new ConcurrentTaskProcessor());
        registry.register(TestEvents.ALWAYS_FAILS, new AlwaysFailsProcessor());
        registry.register(TestEvents.COOL, new DataTaskProcessor());
        registry.register(TestEvents.DEFAULT, new LongTaskProcessor());
        return registry;
    }

    private static class RegularTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            log.info("Processing regular task: {}", task);
            regularTaskExecutions.incrementAndGet();
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private static class DelayedTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            log.info("Processing delayed task: {}", task);
            delayedTaskExecutions.incrementAndGet();
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private static class RecurringTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            log.info("Processing recurring task: {}", task);
            recurringTaskExecutions.incrementAndGet();
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private static class RetryTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            log.info("Processing retry task: {}", task);
            int executions = retryTaskExecutions.incrementAndGet();
            if (executions <= 2) { // Fail first two times
                throw new RuntimeException("Simulated failure for retry testing");
            }
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private static class AlwaysFailsProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            throw new IllegalStateException("Simulated permanent failure");
        }
    }

    private static class ConcurrentTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            try {
                log.info("Processing concurrent task: {}", task);
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static class DataTaskProcessor implements TaskProcessor<Map> {
        @Override
        public void process(Task task, Map data) {
            receivedData.set(data);
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }

        @Override
        public Class<?> dataTypeClass() {
            return Map.class;
        }
    }

    private static class LongTaskProcessor implements TaskProcessor<Void> {
        @Override
        public void process(Task task, Void data) {
            longTaskExecutions.incrementAndGet();
            try {
                Thread.sleep(longTaskMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }
}
