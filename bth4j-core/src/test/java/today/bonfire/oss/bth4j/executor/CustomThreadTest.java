package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class CustomThreadTest {

    /** loops while allowed to continue and sleeps for a long time between iterations */
    private static class SleepyThread extends CustomThread {
        volatile boolean interruptedWhileSleeping = false;
        final CountDownLatch aboutToSleep = new CountDownLatch(1);

        SleepyThread() {
            super(new ThreadGroup("custom-thread-test"), "sleepy");
        }

        @Override
        public void run() {
            try {
                while (canContinueProcessing) {
                    try {
                        aboutToSleep.countDown();
                        Thread.sleep(60_000);
                    } catch (InterruptedException e) {
                        interruptedWhileSleeping = true;
                    }
                }
            } finally {
                doneLatch.countDown();
            }
        }
    }

    @Test
    void stopThreadWakesUpSleepingThreadAndLetsItFinish() throws InterruptedException {
        var thread = new SleepyThread();
        thread.setDaemon(true);
        thread.start();
        assertThat(thread.isRunning()).isTrue();
        // make sure the thread is past its loop check, so stopThread() has to interrupt the sleep
        assertThat(thread.aboutToSleep.await(5, java.util.concurrent.TimeUnit.SECONDS))
                .isTrue();

        long start = System.nanoTime();
        thread.stopThread();

        assertThat(thread.waitTillDone(Duration.ofSeconds(10))).isTrue();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(10));
        assertThat(thread.interruptedWhileSleeping).isTrue();
        thread.join(5000);
        assertThat(thread.isRunning()).isFalse();
    }

    @Test
    void waitTillDoneWithTimeoutReturnsFalseWhileRunning() throws InterruptedException {
        var thread = new SleepyThread();
        thread.setDaemon(true);
        thread.start();
        try {
            assertThat(thread.waitTillDone(Duration.ofMillis(50))).isFalse();
        } finally {
            thread.stopThread();
            thread.waitTillDone();
        }
    }
}
