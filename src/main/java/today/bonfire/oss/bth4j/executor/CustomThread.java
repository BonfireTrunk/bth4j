package today.bonfire.oss.bth4j.executor;

public abstract class CustomThread extends Thread {

  private boolean canContinueProcessing = true;

  public CustomThread(ThreadGroup group, String name) {
    super(group, name);
  }

  public void stopThread() {
    canContinueProcessing = false;
  }


  public boolean canContinueProcessing() {
    return canContinueProcessing;
  }

}
