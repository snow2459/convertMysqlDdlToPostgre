package org.example.pipeline;

/**
 * 针对 JSQLParser 无法识别的 MySQL 扩展语法做预处理，确保后续解析顺利。
 */
public final class SqlPreprocessor {

    private SqlPreprocessor() {
    }

    public static String sanitize(String sql) {
        if (sql == null || sql.isBlank()) {
            return sql;
        }
        String sanitized = removeBinaryLiteralPrefix(sql);
        sanitized = removeUtf8LiteralPrefix(sanitized);
        return sanitized;
    }

    private static String removeBinaryLiteralPrefix(String sql) {
        String result = sql;
        result = result.replaceAll("(?i)_binary\\s*(?=')", "");
        result = result.replaceAll("(?i)_binary\\s*(?=x')", "");
        result = result.replaceAll("(?i)_binary\\s*(?=0x)", "");
        return result;
    }

    private static String removeUtf8LiteralPrefix(String sql) {
        String result = sql;
        result = result.replaceAll("(?i)_utf8mb4\\s*(?=')", "");
        result = result.replaceAll("(?i)_utf8mb4\\s*(?=\"\")", "");
        return result;
    }
}
