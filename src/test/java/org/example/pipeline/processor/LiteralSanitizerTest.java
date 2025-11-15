package org.example.pipeline.processor;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LiteralSanitizerTest {

    @Test
    public void shouldRemoveBinaryPrefixWithSpace() {
        assertEquals("'abc'", LiteralSanitizer.removeBinaryPrefix("_binary 'abc'"));
    }

    @Test
    public void shouldRemoveBinaryPrefixWithoutSpace() {
        assertEquals("x'6162'", LiteralSanitizer.removeBinaryPrefix("_BINARYx'6162'"));
    }

    @Test
    public void shouldKeepNonBinaryLiteral() {
        assertEquals("DEFAULT 0", LiteralSanitizer.removeBinaryPrefix("DEFAULT 0"));
    }
}
