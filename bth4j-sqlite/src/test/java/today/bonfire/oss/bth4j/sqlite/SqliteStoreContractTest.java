package today.bonfire.oss.bth4j.sqlite;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.StoreContract;
import today.bonfire.oss.bth4j.spi.TaskStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * {@link StoreContract} against {@link SqliteTaskStore} on a temporary database file, each test on its own
 * tables and with a clock the test moves. Some contract tests use a second store from {@link #newStore} without
 * calling {@code init()}, so the schema is created here.
 */
class SqliteStoreContractTest extends StoreContract {

    private static SqliteTestSupport database;

    private final MutableClock clock = new MutableClock();

    @BeforeAll
    static void openDatabase() {
        database = SqliteTestSupport.open();
    }

    @AfterAll
    static void closeDatabase() {
        database.close();
    }

    @Override
    protected FullStore newStore(String namespace) {
        return database.store(namespace, clock);
    }

    @Override
    protected Clock clock() {
        return clock;
    }

    @Override
    protected void advance(Duration duration) {
        clock.advance(duration);
    }

    @Override
    protected boolean corruptStoredValue(String namespace, long taskId) {
        try (var c = database.dataSource().getConnection();
                var ps = c.prepareStatement("UPDATE " + SqliteTestSupport.prefix(namespace)
                        + "_task SET headers = '{\"a\":null}' WHERE id = ?")) {
            ps.setLong(1, taskId);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected boolean writeOtherSchemaVersion(String namespace) {
        try (var c = database.dataSource().getConnection();
                var ps = c.prepareStatement("UPDATE " + SqliteTestSupport.prefix(namespace)
                        + "_meta SET value = ? WHERE key = 'schema_version'")) {
            ps.setInt(1, SqliteTaskStore.SCHEMA_VERSION + 1);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected void fillRegistry(TaskStore fresh, String namespace, Registry registry, int lastId) {
        var counter = registry == Registry.QUEUE ? "next_queue_id" : "next_type_id";
        try (var c = database.dataSource().getConnection();
                var ps = c.prepareStatement(
                        "UPDATE " + SqliteTestSupport.prefix(namespace) + "_meta SET value = ? WHERE key = ?")) {
            ps.setLong(1, lastId + 1L);
            ps.setString(2, counter);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
