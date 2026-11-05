package today.bonfire.oss.bth4j.spi;

/** A queue's first-pass share and total cap within one claim request. */
public record QueueGrant(int queueId, int share, int cap) {
    public QueueGrant {
        if (share < 0 || cap < 0 || share > cap) {
            throw new IllegalArgumentException("queue grant requires 0 <= share <= cap");
        }
    }
}
