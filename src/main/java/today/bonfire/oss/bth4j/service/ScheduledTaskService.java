package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.executor.CustomThread;

@Slf4j
public class ScheduledTaskService extends CustomThread {

  private final long     delay;
  private final Runnable runnable;

  public ScheduledTaskService(Builder builder) {
    super(builder.group, builder.threadName);
    this.runnable = builder.runnable;
    this.delay    = builder.delay;
  }

  @Override
  public void run() {
    while (this.canContinueProcessing()) {
      try {
        runnable.run();
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        log.error("Task processor Thread interrupted", e);
      } catch (Exception e) {
        log.error("Task processor Thread error", e);
      }
    }
  }

  public static class Builder {

    private long     delay;
    private Runnable runnable;
    private ThreadGroup group;
    private String      threadName;

    public Builder() {}

    public Builder setGroup(ThreadGroup group) {
      this.group = group;
      return this;
    }

    public Builder setThreadName(String threadName) {
      this.threadName = threadName;
      return this;
    }

    public Builder setDelay(long delay) {
      this.delay = delay;
      return this;
    }

    public Builder setRunnable(Runnable runnable) {
      this.runnable = runnable;
      return this;
    }

    public ScheduledTaskService create() {
      return new ScheduledTaskService(this);
    }

  }
}
