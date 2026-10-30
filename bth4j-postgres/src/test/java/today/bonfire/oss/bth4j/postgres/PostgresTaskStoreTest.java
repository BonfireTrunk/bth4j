package today.bonfire.oss.bth4j.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

class PostgresTaskStoreTest {

    private static final DataSource DATA_SOURCE = mock(DataSource.class);

    @Test
    void nullDataSourceIsRejected() {
        assertThatThrownBy(() -> new PostgresTaskStore(null, "bth")).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void prefixMustBeAPlainLowerCaseIdentifier() {
        for (var bad : new String[] {null, "", "Bth", "1bth", "bth-tasks", "bth tasks", "a".repeat(42)}) {
            assertThatThrownBy(() -> new PostgresTaskStore(DATA_SOURCE, bad))
                    .as("prefix %s", bad)
                    .isInstanceOf(TaskConfigurationError.class);
        }
        assertThatCode(() -> new PostgresTaskStore(DATA_SOURCE, "bth_2")).doesNotThrowAnyException();
    }

    @Test
    void schemaStatementsUseThePrefix() {
        var statements = new PostgresTaskStore(DATA_SOURCE, "app").schemaStatements();

        assertThat(statements).hasSize(8);
        assertThat(statements.getFirst()).startsWith("CREATE TABLE IF NOT EXISTS app_task (");
        assertThat(statements).allSatisfy(s -> assertThat(s).contains("app_"));
        assertThat(statements.getLast()).startsWith("CREATE INDEX IF NOT EXISTS app_schedule_due");
    }

    @Test
    void prefixForNamespaceIsLowerCaseAndSafe() {
        assertThat(PostgresTestSupport.prefix("T12")).isEqualTo("t_t12");
        assertThat(PostgresTestSupport.prefix("BTH-1")).isEqualTo("t_bth_1");
    }
}
