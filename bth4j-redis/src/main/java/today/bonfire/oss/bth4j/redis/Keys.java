package today.bonfire.oss.bth4j.redis;

/**
 * Redis key names of one runner. Every key carries the namespace as a hash tag so that all of them
 * live in the same Redis Cluster slot, which scripts and multi-key commands require.
 */
public final class Keys {
    public final String namespace;
    public final String scheduled;
    public final String leased;
    public final String dead;
    public final String cron;
    /** counter behind recurring definition revisions; it never resets, so a recreated definition cannot reuse one */
    public final String cronRevision;

    public final String taskPrefix;
    public final String queuePrefix;
    public final String cronPrefix;

    public Keys(String namespace) {
        this.namespace = namespace;
        var p = "{" + namespace + "}:";
        this.scheduled = p + "scheduled";
        this.leased = p + "leased";
        this.dead = p + "dead";
        this.cron = p + "cron";
        this.cronRevision = p + "cronrev";
        this.taskPrefix = p + "t:";
        this.queuePrefix = p + "q:";
        this.cronPrefix = p + "cron:";
    }

    public String task(String id) {
        return taskPrefix + id;
    }

    public String queue(String queueName) {
        return queuePrefix + queueName;
    }

    public String cronDefinition(String scheduleId) {
        return cronPrefix + scheduleId;
    }
}
