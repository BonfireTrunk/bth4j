package today.bonfire.oss.bth4j.sqlite;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.Locale;
import javax.sql.DataSource;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * A SQLite database file in a temporary directory with a small connection pool, for one test class. The pool
 * opens connections in WAL mode with a busy timeout, the settings {@link SqliteTaskStore#init()} requires.
 * Needs no Docker.
 */
final class SqliteTestSupport implements AutoCloseable {
    private static final int POOL_SIZE = 32;

    private final Path directory;
    private final Path file;
    private final HikariDataSource dataSource;

    private SqliteTestSupport(Path directory, Path file, HikariDataSource dataSource) {
        this.directory = directory;
        this.file = file;
        this.dataSource = dataSource;
    }

    static SqliteTestSupport open() {
        try {
            var directory = Files.createTempDirectory("bth4j-sqlite");
            var file = directory.resolve("tasks.db");
            return new SqliteTestSupport(directory, file, pool(url(file, "journal_mode=WAL&busy_timeout=5000"), true));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String url(Path file, String parameters) {
        return "jdbc:sqlite:" + file + (parameters.isEmpty() ? "" : "?" + parameters);
    }

    /** A pool on the JDBC URL; auto-commit is what the pool hands out, so a test can choose it. */
    static HikariDataSource pool(String url, boolean autoCommit) {
        return pool(url, autoCommit, POOL_SIZE);
    }

    static HikariDataSource pool(String url, boolean autoCommit, int size) {
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setMaximumPoolSize(size);
        config.setMinimumIdle(size);
        config.setAutoCommit(autoCommit);
        return new HikariDataSource(config);
    }

    DataSource dataSource() {
        return dataSource;
    }

    /** Another pool on the same file, as a second process would have; the caller closes it. */
    HikariDataSource anotherPool() {
        return pool(url(file, "journal_mode=WAL&busy_timeout=5000"), true);
    }

    Path file() {
        return file;
    }

    /** A store on its own tables, named after the namespace, with the schema created. */
    SqliteTaskStore store(String namespace, Clock clock) {
        var store = new SqliteTaskStore(dataSource, prefix(namespace), clock);
        store.init();
        return store;
    }

    /** A task that is ready to run, created at {@code now}. */
    static TaskRecord.Builder task(long id, int queueId, int typeId, Instant now) {
        return TaskRecord.builder()
                .id(id)
                .queueId(queueId)
                .typeId(typeId)
                .maxAttempts(3)
                .createdAt(now)
                .encodedPayload("payload-" + id);
    }

    static String prefix(String namespace) {
        return "t_" + namespace.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
    }

    @Override
    public void close() {
        dataSource.close();
        try (var files = Files.walk(directory)) {
            files.sorted(Comparator.reverseOrder())
                    .forEach(path -> path.toFile().delete());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
