package today.bonfire.oss.bth4j.spi;

import java.util.List;

/**
 * One page of a task listing.
 *
 * @param next the cursor for the following page, or null when this page was the last one
 */
public record TaskPage(List<TaskRecord> items, String next) {
    public TaskPage {
        items = List.copyOf(items);
    }
}
