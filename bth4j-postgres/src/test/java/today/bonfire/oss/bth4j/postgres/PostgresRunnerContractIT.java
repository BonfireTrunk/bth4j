package today.bonfire.oss.bth4j.postgres;

import today.bonfire.oss.bth4j.RunnerContract;
import today.bonfire.oss.bth4j.spi.FullStore;

/** The shared runner contract run against {@link PostgresTaskStore} on a real PostgreSQL. Needs Docker. */
class PostgresRunnerContractIT extends RunnerContract {

    @Override
    protected FullStore newStore(String namespace) {
        return PostgresTestSupport.store(namespace);
    }
}
