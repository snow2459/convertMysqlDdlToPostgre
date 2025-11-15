package org.example.pipeline;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SqlPreprocessorTest {

    @Test
    public void shouldRemoveBinaryPrefixBeforeStringLiterals() {
        String sql = "INSERT INTO t VALUES (_binary 'abc');";
        String sanitized = SqlPreprocessor.sanitize(sql);
        assertEquals("INSERT INTO t VALUES ('abc');", sanitized);
    }

    @Test
    public void shouldRemoveBinaryPrefixBeforeHexLiteral() {
        String sql = "INSERT INTO t VALUES (_binary x'6162');";
        String sanitized = SqlPreprocessor.sanitize(sql);
        assertEquals("INSERT INTO t VALUES (x'6162');", sanitized);
    }
}
