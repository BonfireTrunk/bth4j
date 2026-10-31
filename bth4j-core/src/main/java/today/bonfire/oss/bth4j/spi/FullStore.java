package today.bonfire.oss.bth4j.spi;

/** What a store module implements: every capability the runner, the client and the admin API need. */
public interface FullStore extends ExecutionStore, MaintenanceStore, ScheduleStore, InspectionStore, AdminStore {}
