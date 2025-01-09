package today.bonfire.oss.bth4j;

/**
 * This is the base interface for all the events that are to be used with
 * {@link Task}.
 * Usually your events would be enums that implement this interface.
 */
public interface Event {

  /**
   * The value that is used to identify the event. And this value is part of the task
   */
  int value();

  /**
   * This is used to identify if the task is recurring or not.
   * if you don't want to support recurring tasks, you can return false by default
   *
   * @return true if the task is recurring
   */
  boolean isRecurring();


  int retryCount();

}
