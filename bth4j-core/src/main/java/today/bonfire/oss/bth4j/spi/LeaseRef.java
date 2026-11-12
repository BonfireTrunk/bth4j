package today.bonfire.oss.bth4j.spi;

/** A lease a runner believes it holds: the task and the revision its claim produced. */
public record LeaseRef(long id, int revision) {}
