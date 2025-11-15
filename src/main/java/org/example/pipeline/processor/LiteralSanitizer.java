package org.example.pipeline.processor;

import java.util.Locale;

/**
 * 针对特殊字面量的规整工具。
 */
final class LiteralSanitizer {

    private LiteralSanitizer() {
    }

    static String removeBinaryPrefix(String literal) {
        if (literal == null) {
            return null;
        }
        String trimmed = literal.trim();
        if (trimmed.toLowerCase(Locale.ROOT).startsWith("_binary")) {
            String remaining = trimmed.substring(7).stripLeading();
            return remaining.isEmpty() ? "" : remaining;
        }
        return literal;
    }
}
