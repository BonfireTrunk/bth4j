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
    protected boolean corruptStoredValue(String namespace, long taskId) {
        try (var c = PostgresTestSupport.dataSource().getConnection();
                var ps = c.prepareStatement("UPDATE " + PostgresTestSupport.prefix(namespace)
                        + "_task SET headers = '{\"a\":null}'::jsonb WHERE id = ?")) {
            ps.setLong(1, taskId);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected boolean writeOtherSchemaVersion(String namespace) {
        try (var c = PostgresTestSupport.dataSource().getConnection();
                var ps = c.prepareStatement("UPDATE " + PostgresTestSupport.prefix(namespace)
                        + "_meta SET value = ? WHERE key = 'schema_version'")) {
            ps.setString(1, String.valueOf(PostgresTaskStore.SCHEMA_VERSION + 1));
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
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
