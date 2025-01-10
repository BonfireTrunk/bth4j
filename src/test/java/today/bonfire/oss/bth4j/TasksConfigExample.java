package today.bonfire.oss.bth4j;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.service.Task;
import today.bonfire.oss.bth4j.service.TaskProcessorRegistry;

@Slf4j
public class TasksConfigExample {
  public static TaskProcessorRegistry newTaskProcessorRegistry() {
    TaskProcessorRegistry registry = new TaskProcessorRegistry();
    registry.register(TestEvents.UNKNOWN, new VoidTask2());
    registry.register(TestEvents.COOL, new CoolTask());
    registry.register(TestEvents.NO_DATA, new NoDataTask());
    registry.register(TestEvents.DEFAULT, new DefaultTask());
    registry.register(TestEvents.RECURRING, new RecurringTask());
    return registry;
  }

  private static class NoDataTask implements TaskProcessor<Void> {

    @Override public void process(Task task, Void data) {
      // do nothing
      Math.min(2, 2);
    }
  }

  private static class DefaultTask implements TaskProcessor<String> {

    @Override public void process(Task task, String data) {
      log.info("Processing task {}", task.taskString());
      log.info("Task data: {}", data);
    }

    @Override public Class<String> dataTypeClass() {
      return String.class;
    }

    @Override public boolean requiresData() {
      return true;
    }
  }

  private static class VoidTask2 implements TaskProcessor<Void> {

    @Override public void process(Task task, Void data) {
      // do nothing
    }
  }

  private static class RecurringTask implements TaskProcessor<Void> {

    @Override public void process(Task task, Void data) {
      log.info("Processing recurring task {}", task.taskString());
    }

    @Override public Class<Void> dataTypeClass() {
      return TaskProcessor.super.dataTypeClass();
    }

    @Override public boolean requiresData() {
      return TaskProcessor.super.requiresData();
    }
  }

  private static class CoolTask implements TaskProcessor<Integer> {

    @Override public void process(Task task, Integer data) {
      Math.min(1, 2);
      log.info("Processing cool task {} with data {}", task.taskString(), data);
    }

    @Override public Class<Integer> dataTypeClass() {
      return Integer.class;
    }

    @Override public boolean requiresData() {
      return true;
    }
  }
}
