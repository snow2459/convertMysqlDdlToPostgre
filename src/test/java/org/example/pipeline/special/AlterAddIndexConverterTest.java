package org.example.pipeline.special;

import org.example.pipeline.dialect.gauss.GaussMySqlDialectProfile;
import org.example.pipeline.dialect.postgres.PostgreSqlDialectProfile;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlterAddIndexConverterTest {

    @Test
    public void shouldConvertToCreateIndexForPostgres() {
        String sql = "ALTER TABLE demo ADD INDEX idx_demo (col1, col2 DESC);";
        List<String> statements = AlterAddIndexConverter.tryConvert(sql, new PostgreSqlDialectProfile());
        assertEquals(1, statements.size());
        assertTrue(statements.get(0).contains("CREATE INDEX idx_demo ON demo (col1, col2 DESC);"));
    }

    @Test
    public void shouldKeepAlterStatementForGauss() {
        String sql = "ALTER TABLE demo ADD INDEX idx_demo (col1, col2 DESC);";
        List<String> statements = AlterAddIndexConverter.tryConvert(sql, new GaussMySqlDialectProfile());
        assertEquals(1, statements.size());
        assertTrue(statements.get(0).startsWith("ALTER TABLE demo"));
    }
}
