package org.example.pipeline;

/**
 * 累积转换后的 SQL 文本，保持原始顺序。
 */
public class ConversionResult {

    private final StringBuilder builder = new StringBuilder();

    public void appendStatement(String sql) {
        if (sql == null || sql.isBlank()) {
            return;
        }
        builder.append(sql.stripTrailing());
        if (!sql.stripTrailing().endsWith(";")) {
            builder.append(";");
        }
        builder.append("\n");
    }

    public void appendRaw(String raw) {
        builder.append(raw);
    }

    public String asSql() {
        return builder.toString();
    }
}
