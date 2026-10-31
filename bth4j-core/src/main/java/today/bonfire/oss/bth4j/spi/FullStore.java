package today.bonfire.oss.bth4j.spi;

/** What a store module implements: every capability the runner and the client need. */
public interface FullStore extends ExecutionStore, MaintenanceStore, ScheduleStore, InspectionStore {}
