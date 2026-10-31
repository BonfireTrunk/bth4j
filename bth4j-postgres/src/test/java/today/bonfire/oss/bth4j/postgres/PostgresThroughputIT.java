package today.bonfire.oss.bth4j.postgres;

import today.bonfire.oss.bth4j.ThroughputContract;
import today.bonfire.oss.bth4j.spi.FullStore;

/** The shared throughput contract run against {@link PostgresTaskStore} on a real PostgreSQL. Needs Docker. */
class PostgresThroughputIT extends ThroughputContract {

    @Override
    protected FullStore newStore(String namespace) {
        return PostgresTestSupport.store(namespace);
    }
}
