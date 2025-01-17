package today.bonfire.oss.bth4j;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum TestEvents implements Event {
  UNKNOWN(0, 0),
  COOL(1, 3),
  NO_DATA(2, 0),
  REGULAR_TASK(3, 0),
  DELAYED_TASK(4, 0),
  RETRY_TASK(5, 3),
  CONCURRENT_TASK(6, 0),
  DEFAULT(10, 1),
  RECURRING(20, 2),
  RECURRING_TASK(200, 0),
  ;

  private final static Map<String, TestEvents> internalMap =
      Arrays.stream(TestEvents.values())
            .map(e -> Map.entry(e.value(), e))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  private final        int                     value;
  private final        int                     retryCount;

  TestEvents(int value, int retryCount) {
    this.value      = value;
    this.retryCount = retryCount;
  }

  @Override
  public boolean isRecurring() {
    return this.value > DEFAULT.value;
  }

  public int retryCount() {
    return retryCount;
  }

  @Override
  public String value() {
    return String.valueOf(this.value);
  }

  @Override
  public Event from(String value) {
    return internalMap.getOrDefault(value, UNKNOWN);
  }
}
