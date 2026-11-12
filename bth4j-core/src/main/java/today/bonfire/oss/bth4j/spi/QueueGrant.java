package today.bonfire.oss.bth4j.spi;

import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/** A queue's first-pass share and total cap within one claim request. */
public record QueueGrant(int queueId, int share, int cap) {
    public QueueGrant {
        if (share < 0 || cap < 0 || share > cap) {
            throw new TaskValidationException("queue grant requires 0 <= share <= cap");
        }
    }
}
