package org.example.pipeline.special;

import org.example.pipeline.DatabaseDialect;
import org.example.pipeline.GaussMySqlDialect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * 针对 JSQLParser 暂不支持的 "ALTER TABLE ... ADD INDEX (... DESC)" 语句，
 * 提供手动解析与转换能力。
 */
final class AlterAddIndexConverter {

    private AlterAddIndexConverter() {
    }

    static List<String> tryConvert(String rawSql, DatabaseDialect dialect) {
        ParsedAlterAddIndex parsed = parse(rawSql);
        if (parsed == null) {
            return Collections.emptyList();
        }
        if (dialect instanceof GaussMySqlDialect) {
            return List.of(parsed.toAlterStatement());
        }
        return parsed.toCreateIndexStatements();
    }

    private static ParsedAlterAddIndex parse(String rawSql) {
        if (rawSql == null) {
            return null;
        }
        String trimmed = rawSql.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        // 去掉末尾分号
        if (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        String normalized = trimmed.replace("\r", " ").replace("\n", " ");
        normalized = normalized.replaceAll("\\s+", " ");
        String upper = normalized.toUpperCase(Locale.ROOT);
        if (!upper.startsWith("ALTER TABLE")) {
            return null;
        }

        int tableStart = "ALTER TABLE".length();
        int nextSpace = normalized.indexOf(' ', tableStart + 1);
        if (nextSpace == -1) {
            return null;
        }
        String tableName = normalized.substring(tableStart, nextSpace).trim();
        String rest = normalized.substring(nextSpace).trim();
        if (tableName.isEmpty() || !rest.toUpperCase(Locale.ROOT).startsWith("ADD")) {
            return null;
        }

        List<IndexDefinition> indexDefinitions = splitSegments(rest);
        if (indexDefinitions.isEmpty()) {
            return null;
        }
        return new ParsedAlterAddIndex(tableName, indexDefinitions);
    }

    private static List<IndexDefinition> splitSegments(String rest) {
        List<IndexDefinition> definitions = new ArrayList<>();
        String working = rest.replaceAll("\\)\\s*,\\s*ADD", ")#SPLIT#ADD");
        String[] segments = working.split("#SPLIT#");
        for (String segment : segments) {
            IndexDefinition definition = parseSegment(segment.trim());
            if (definition == null) {
                return Collections.emptyList();
            }
            definitions.add(definition);
        }
        return definitions;
    }

    private static IndexDefinition parseSegment(String segment) {
        if (segment.toUpperCase(Locale.ROOT).startsWith("ADD ")) {
            segment = segment.substring(4).trim();
        } else {
            return null;
        }
        boolean unique = false;
        if (segment.toUpperCase(Locale.ROOT).startsWith("UNIQUE ")) {
            unique = true;
            segment = segment.substring(7).trim();
        }
        if (!segment.toUpperCase(Locale.ROOT).startsWith("INDEX ")) {
            return null;
        }
        segment = segment.substring(6).trim();

        int parenStart = segment.indexOf('(');
        if (parenStart == -1) {
            return null;
        }
        String beforeParen = segment.substring(0, parenStart).trim();
        if (beforeParen.isEmpty()) {
            return null;
        }
        String indexName;
        String method = null;
        int usingIdx = beforeParen.toUpperCase(Locale.ROOT).indexOf(" USING ");
        if (usingIdx != -1) {
            indexName = beforeParen.substring(0, usingIdx).trim();
            method = beforeParen.substring(usingIdx + 7).trim();
        } else {
            indexName = beforeParen;
        }

        String columnsPartWithParen = segment.substring(parenStart);
        int closing = findMatchingParen(columnsPartWithParen, 0);
        if (closing == -1) {
            return null;
        }
        String columnBody = columnsPartWithParen.substring(1, closing);
        List<String> columns = splitColumnList(columnBody);
        if (columns.isEmpty()) {
            return null;
        }

        return new IndexDefinition(indexName, unique, method, columns);
    }

    private static int findMatchingParen(String text, int start) {
        if (start >= text.length() || text.charAt(start) != '(') {
            return -1;
        }
        int depth = 0;
        for (int i = start; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static List<String> splitColumnList(String body) {
        List<String> columns = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            if (c == ',' && depth == 0) {
                String column = current.toString().trim();
                if (!column.isEmpty()) {
                    columns.add(column);
                }
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        if (current.length() > 0) {
            String column = current.toString().trim();
            if (!column.isEmpty()) {
                columns.add(column);
            }
        }
        return columns;
    }

    private static final class ParsedAlterAddIndex {
        private final String tableName;
        private final List<IndexDefinition> indexes;

        ParsedAlterAddIndex(String tableName, List<IndexDefinition> indexes) {
            this.tableName = tableName;
            this.indexes = indexes;
        }

        String toAlterStatement() {
            StringBuilder builder = new StringBuilder();
            builder.append("ALTER TABLE ").append(tableName).append("\n");
            for (int i = 0; i < indexes.size(); i++) {
                IndexDefinition index = indexes.get(i);
                builder.append("    ADD ");
                if (index.unique) {
                    builder.append("UNIQUE ");
                }
                builder.append("INDEX ").append(index.indexName)
                        .append(" (").append(String.join(", ", index.columns)).append(")");
                if (i != indexes.size() - 1) {
                    builder.append(",\n");
                } else {
                    builder.append(";");
                }
            }
            return builder.toString();
        }

        List<String> toCreateIndexStatements() {
            List<String> statements = new ArrayList<>();
            for (IndexDefinition index : indexes) {
                StringBuilder builder = new StringBuilder();
                builder.append("CREATE ");
                if (index.unique) {
                    builder.append("UNIQUE ");
                }
                builder.append("INDEX ").append(index.indexName)
                        .append(" ON ").append(tableName)
                        .append(" (").append(String.join(", ", index.columns)).append(");");
                statements.add(builder.toString());
            }
            return statements;
        }
    }

    private static final class IndexDefinition {
        private final String indexName;
        private final boolean unique;
        private final String method;
        private final List<String> columns;

        IndexDefinition(String indexName, boolean unique, String method, List<String> columns) {
            this.indexName = indexName;
            this.unique = unique;
            this.method = method;
            this.columns = columns;
        }
    }
}
