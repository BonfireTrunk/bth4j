package today.bonfire.oss.bth4j;

import java.util.List;

/**
 * One page of the schedule listing.
 *
 * @param next the cursor for the following page, or null when this page was the last one
 */
public record ScheduleViewPage(List<ScheduleView> items, String next) {
    public ScheduleViewPage {
        items = List.copyOf(items);
    }
}
