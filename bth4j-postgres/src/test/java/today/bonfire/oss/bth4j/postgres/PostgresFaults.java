package today.bonfire.oss.bth4j.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import today.bonfire.oss.bth4j.fault.FaultPlan;
import today.bonfire.oss.bth4j.fault.Faulty;
import today.bonfire.oss.bth4j.fault.FaultyDataSource;

/**
 * Stores on the PostgreSQL of {@link PostgresTestSupport} whose connections a {@link FaultPlan} can cut. The pool
 * of the store under test sits on top of the cutting layer, so it sees a cut as it sees a lost connection.
 */
final class PostgresFaults implements AutoCloseable {
    private final FaultPlan plan = new FaultPlan();
    private final HikariDataSource pool;

    PostgresFaults() {
        var driver = new PGSimpleDataSource();
        driver.setUrl(PostgresTestSupport.jdbcUrl());
        driver.setUser(PostgresTestSupport.username());
        driver.setPassword(PostgresTestSupport.password());
        var config = new HikariConfig();
        config.setDataSource(FaultyDataSource.around(driver, plan));
        config.setMaximumPoolSize(8);
        pool = new HikariDataSource(config);
    }

    /** A store of the namespace that the plan cuts, and one that it never does. */
    Faulty faulty(String namespace) {
        var prefix = PostgresTestSupport.prefix(namespace);
        var plain = PostgresTaskStore.builder()
                .dataSource(PostgresTestSupport.dataSource())
                .namespace(prefix)
                .build();
        var store =
                PostgresTaskStore.builder().dataSource(pool).namespace(prefix).build();
        return new Faulty(store, plain, plan);
    }

    @Override
    public void close() {
        pool.close();
    }
}
