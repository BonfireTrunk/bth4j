package today.bonfire.oss.bth4j;

import java.util.List;

/**
 * One page of a task listing.
 *
 * @param next the cursor for the following page, or null when this page was the last one
 */
public record TaskViewPage(List<TaskView> items, String next) {
    public TaskViewPage {
        items = List.copyOf(items);
    }
}
