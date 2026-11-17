package today.bonfire.oss.bth4j.sqlite;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Clock;
import org.sqlite.SQLiteDataSource;
import today.bonfire.oss.bth4j.fault.FaultPlan;
import today.bonfire.oss.bth4j.fault.Faulty;
import today.bonfire.oss.bth4j.fault.FaultyDataSource;

/**
 * Stores on the database of a {@link SqliteTestSupport} whose connections a {@link FaultPlan} can cut. The pool of
 * the store under test sits on top of the cutting layer, so it sees a cut as it sees a lost connection.
 */
final class SqliteFaults implements AutoCloseable {
    private final SqliteTestSupport database;
    private final FaultPlan plan = new FaultPlan();
    private final HikariDataSource pool;

    SqliteFaults(SqliteTestSupport database) {
        this.database = database;
        var driver = new SQLiteDataSource();
        driver.setUrl(SqliteTestSupport.url(database.file(), "journal_mode=WAL&busy_timeout=5000"));
        var config = new HikariConfig();
        config.setDataSource(FaultyDataSource.around(driver, plan));
        config.setMaximumPoolSize(8);
        pool = new HikariDataSource(config);
    }

    /** A store of the namespace that the plan cuts, and one that it never does, on {@code clock}. */
    Faulty faulty(String namespace, Clock clock) {
        var prefix = SqliteTestSupport.prefix(namespace);
        var plain = SqliteTaskStore.builder()
                .dataSource(database.dataSource())
                .namespace(prefix)
                .clock(clock)
                .build();
        var store = SqliteTaskStore.builder()
                .dataSource(pool)
                .namespace(prefix)
                .clock(clock)
                .build();
        return new Faulty(store, plain, plan);
    }

    @Override
    public void close() {
        pool.close();
    }
}
