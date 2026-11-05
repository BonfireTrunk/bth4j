package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Base class for the long running service threads of the background runner.
 * Subclasses loop while {@link #canContinueProcessing} is true and must count down
 * {@link #doneLatch} (in a finally block) when they exit.
 */
abstract class CustomThread extends Thread {

    protected final CountDownLatch doneLatch = new CountDownLatch(1);

    /** written by the thread that calls {@link #stopThread()}, read by the service loop, so it must be volatile */
    protected volatile boolean canContinueProcessing = true;

    CustomThread(ThreadGroup group, String name) {
        super(group, name);
    }

    /**
     * Signals the thread to stop and interrupts it so that a thread sleeping between
     * iterations wakes up immediately. Use {@link #waitTillDone()} to wait for the thread to exit.
     */
    void stopThread() {
        canContinueProcessing = false;
        interrupt();
    }

    boolean isRunning() {
        return isAlive();
    }

    /**
     * Blocks until the service loop has exited.
     */
    void waitTillDone() throws InterruptedException {
        doneLatch.await();
    }

    /**
     * Blocks until the service loop has exited or the timeout elapses.
     *
     * @return true if the service loop exited, false if the timeout elapsed first
     */
    boolean waitTillDone(Duration timeout) throws InterruptedException {
        return doneLatch.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
}
