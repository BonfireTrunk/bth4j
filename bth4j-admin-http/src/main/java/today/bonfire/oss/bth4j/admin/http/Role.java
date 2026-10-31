package today.bonfire.oss.bth4j.admin.http;

/** What a signed-in user may do. */
public enum Role {
    /** may read everything and change tasks, queues and schedules */
    ADMIN,
    /** may read everything and sign out; every other change is refused with 403 */
    VIEWER
}
