package today.bonfire.oss.bth4j.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.QueueGrant;

class PostgresTaskStoreTest {

    private static final DataSource DATA_SOURCE = mock(DataSource.class);

    @Test
    void nullDataSourceIsRejected() {
        assertThatThrownBy(() -> new PostgresTaskStore(null, "bth")).isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void prefixMustBeAPlainLowerCaseIdentifier() {
        for (var bad : new String[] {null, "", "Bth", "1bth", "bth-tasks", "bth tasks", "a".repeat(42)}) {
            assertThatThrownBy(() -> new PostgresTaskStore(DATA_SOURCE, bad))
                    .as("prefix %s", bad)
                    .isInstanceOf(TaskConfigurationException.class);
        }
        assertThatCode(() -> new PostgresTaskStore(DATA_SOURCE, "bth_2")).doesNotThrowAnyException();
    }

    @Test
    void schemaStatementsUseThePrefix() {
        var statements = new PostgresTaskStore(DATA_SOURCE, "app").schemaStatements();

        assertThat(statements).hasSize(17);
        assertThat(statements.getFirst()).startsWith("CREATE TABLE IF NOT EXISTS app_meta (");
        assertThat(statements).allSatisfy(s -> assertThat(s).contains("app_"));
        assertThat(statements).noneSatisfy(s -> assertThat(s).containsIgnoringCase("REFERENCES"));
        assertThat(statements).noneSatisfy(s -> assertThat(s).containsIgnoringCase("ALTER"));
        assertThat(statements).noneSatisfy(s -> assertThat(s).contains("event"));
        assertThat(statements)
                .contains(
                        "CREATE TABLE IF NOT EXISTS app_type (id smallserial PRIMARY KEY, name text NOT NULL UNIQUE)",
                        "CREATE INDEX IF NOT EXISTS app_task_queued ON app_task (queue_id, due, id, type_id) WHERE state = 0");
        assertThat(statements)
                .filteredOn(s -> s.startsWith("CREATE TABLE IF NOT EXISTS app_worker"))
                .singleElement()
                .asString()
                .contains(
                        "accepting boolean NOT NULL", "queue_ids smallint[] NOT NULL", "type_ids smallint[] NOT NULL");
        assertThat(statements)
                .filteredOn(s -> s.startsWith("CREATE TABLE IF NOT EXISTS app_task")
                        || s.startsWith("CREATE TABLE IF NOT EXISTS app_schedule"))
                .hasSize(2)
                .allSatisfy(s -> assertThat(s).contains("type_id smallint NOT NULL"));
        assertThat(statements.getLast()).startsWith("INSERT INTO app_meta");
    }

    @Test
    void emptyClaimsDoNotOpenAConnection() {
        var dataSource = mock(DataSource.class);
        var store = new PostgresTaskStore(dataSource, "app");
        var grants = List.of(new QueueGrant(1, 1, 2));
        assertThat(store.claim(new ClaimRequest(grants, 0, Duration.ofSeconds(30), 1, null)))
                .isEmpty();
        assertThat(store.claim(new ClaimRequest(grants, 1, Duration.ofSeconds(30), 1, Set.of())))
                .isEmpty();
        verifyNoInteractions(dataSource);
    }

    @Test
    void prefixForNamespaceIsLowerCaseAndSafe() {
        assertThat(PostgresTestSupport.prefix("T12")).isEqualTo("t_t12");
        assertThat(PostgresTestSupport.prefix("BTH-1")).isEqualTo("t_bth_1");
    }
}
