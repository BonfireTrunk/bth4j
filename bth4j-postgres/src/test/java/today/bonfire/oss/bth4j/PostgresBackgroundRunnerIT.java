package today.bonfire.oss.bth4j;

import today.bonfire.oss.bth4j.postgres.PostgresTestSupport;
import today.bonfire.oss.bth4j.store.TaskStore;

/** {@link BackgroundRunnerContract} against a real PostgreSQL. Needs Docker. */
class PostgresBackgroundRunnerIT extends BackgroundRunnerContract {

    @Override
    protected TaskStore newStore(String namespace) {
        return PostgresTestSupport.store(namespace);
    }
}
