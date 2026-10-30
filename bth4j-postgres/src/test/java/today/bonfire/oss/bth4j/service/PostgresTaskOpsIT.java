package today.bonfire.oss.bth4j.service;

import today.bonfire.oss.bth4j.postgres.PostgresTestSupport;
import today.bonfire.oss.bth4j.store.TaskStore;

/** {@link TaskOpsContract} against a real PostgreSQL. Needs Docker. */
class PostgresTaskOpsIT extends TaskOpsContract {

    @Override
    protected TaskStore newStore(String namespace) {
        return PostgresTestSupport.store(namespace);
    }
}
