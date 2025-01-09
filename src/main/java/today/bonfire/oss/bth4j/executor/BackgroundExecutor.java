package today.bonfire.oss.bth4j.executor;

import java.util.concurrent.ExecutorService;

public interface BackgroundExecutor {

  ExecutorService getExecutor();

  boolean isPoolFull();

  void execute(Runnable task);

  void shutdown();
}
