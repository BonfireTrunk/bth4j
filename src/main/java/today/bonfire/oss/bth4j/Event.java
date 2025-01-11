package today.bonfire.oss.bth4j;

import today.bonfire.oss.bth4j.service.Task;

/**
 * This is the base interface for all the events that are to be used with
 * {@link Task}.
 * Your events can be enums that implement this interface,
 * or you can create your own special class.
 */
public interface Event {

  /**
   * This is used to identify if the task is recurring or not.
   * if you don't want to support recurring tasks, you can return false by default
   *
   * @return true if the task is recurring
   */
  boolean isRecurring();


  int retryCount();

  /**
   * Returns the value of the event.
   * This value is the identifier of the event,
   * and is used to identify the event and stored as part of the task.
   *
   * @return the value of the event
   */
  String value();

  /**
   * Get the event from the given value.
   * {@link Event#value()} is the value of the event that would
   * be suppied to this method
   *
   * @param value value of the event
   * @return the event
   */
  Event from(String value);


}
