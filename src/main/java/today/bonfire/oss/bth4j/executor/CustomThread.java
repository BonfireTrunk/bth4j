package today.bonfire.oss.bth4j.executor;

import java.util.concurrent.CountDownLatch;

public abstract class CustomThread extends Thread {

  protected final CountDownLatch doneLatch = new CountDownLatch(1);

  protected boolean canContinueProcessing = true;

  public CustomThread(ThreadGroup group, String name) {
    super(group, name);
  }

  public void stopThread() {
    canContinueProcessing = false;
  }

  public boolean isRunning() {
    return isAlive();
  }

  public void waitTillDone() throws InterruptedException {
      doneLatch.await();
  }

}
