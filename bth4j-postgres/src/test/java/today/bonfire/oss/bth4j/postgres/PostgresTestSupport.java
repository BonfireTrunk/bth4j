package today.bonfire.oss.bth4j.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Locale;
import javax.sql.DataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** One PostgreSQL container and one pool for all integration tests of the module. Needs Docker. */
public final class PostgresTestSupport {
    private static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer("postgres:17-alpine");
    private static HikariDataSource dataSource;

    private PostgresTestSupport() {}

    public static synchronized DataSource dataSource() {
        if (dataSource == null) {
            CONTAINER.start();
            var config = new HikariConfig();
            config.setJdbcUrl(CONTAINER.getJdbcUrl());
            config.setUsername(CONTAINER.getUsername());
            config.setPassword(CONTAINER.getPassword());
            config.setMaximumPoolSize(32);
            dataSource = new HikariDataSource(config);
        }
        return dataSource;
    }

    /** A store on its own tables, named after the namespace, with the schema created. */
    public static PostgresTaskStore store(String namespace) {
        var store = new PostgresTaskStore(dataSource(), prefix(namespace));
        store.createSchema();
        return store;
    }

    static String prefix(String namespace) {
        return "t_" + namespace.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
    }
}
