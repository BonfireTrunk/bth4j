package today.bonfire.oss.bth4j.postgres;

import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.StoreContract;

/**
 * {@link StoreContract} against a real PostgreSQL, each test on its own tables. Some contract tests use a
 * second store from {@link #newStore} without calling {@code init()}, so the schema is created here.
 * Needs Docker.
 */
class PostgresStoreContractIT extends StoreContract {

    @Override
    protected FullStore newStore(String namespace) {
        return PostgresTestSupport.store(namespace);
    }
}
