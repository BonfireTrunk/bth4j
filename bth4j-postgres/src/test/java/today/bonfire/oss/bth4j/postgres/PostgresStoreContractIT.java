package today.bonfire.oss.bth4j.postgres;

import java.sql.SQLException;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.StoreContract;
import today.bonfire.oss.bth4j.spi.TaskStore;

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

    @Override
    protected void fillRegistry(TaskStore fresh, String namespace, Registry registry, int lastId) {
        var sequence =
                PostgresTestSupport.prefix(namespace) + (registry == Registry.QUEUE ? "_queue" : "_type") + "_id_seq";
        try (var c = PostgresTestSupport.dataSource().getConnection();
                var st = c.createStatement()) {
            st.execute("SELECT setval('" + sequence + "', " + lastId + ")");
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
