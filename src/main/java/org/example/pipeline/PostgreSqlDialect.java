package org.example.pipeline;

import java.util.Locale;

/**
 * PostgreSQL 方言实现，后续可继续扩展（如大小写策略、关键字映射等）。
 */
public class PostgreSqlDialect implements DatabaseDialect {

    @Override
    public String getName() {
        return "postgresql";
    }

    @Override
    public String formatBoolean(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public String maybeQuoteIdentifier(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            return identifier;
        }
        String normalized = identifier.trim();
        // 简单判断：包含大写或特殊字符时加双引号，其余保持裸露，方便阅读。
        if (needsQuoting(normalized)) {
            return "\"" + normalized.replace("\"", "\"\"") + "\"";
        }
        return normalized;
    }

    private boolean needsQuoting(String identifier) {
        boolean hasUpperCase = !identifier.equals(identifier.toLowerCase(Locale.ROOT));
        boolean startsWithDigit = Character.isDigit(identifier.charAt(0));
        boolean containsSpecial = identifier.chars()
                .anyMatch(ch -> !(Character.isLetterOrDigit(ch) || ch == '_'));
        return hasUpperCase || startsWithDigit || containsSpecial;
    }
}
