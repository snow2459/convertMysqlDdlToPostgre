package org.example.pipeline.converter.postgres;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import org.example.pipeline.converter.AbstractCreateTableConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * PostgreSQL 方言下的建表转换器。
 */
public class PostgreSqlCreateTableConverter extends AbstractCreateTableConverter {

    @Override
    public String convert(CreateTable createTable) throws JSQLParserException {
        String tableFullyQualifiedName = createTable.getTable().getFullyQualifiedName();
        List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();

        List<String> tableOptionsStrings = createTable.getTableOptionsStrings();
        String tableCommentSql = extractTableComment(tableFullyQualifiedName, tableOptionsStrings);

        List<String> columnComments = extractColumnCommentSql(tableFullyQualifiedName, columnDefinitions);

        Index primaryKey = resolvePrimaryKey(columnDefinitions, () -> resolvePrimaryKeyFromIndex(createTable));
        if (primaryKey == null) {
            throw new IllegalStateException("Primary key not found");
        }

        String createTableFirstLine = String.format("CREATE TABLE %s (", tableFullyQualifiedName);
        String primaryKeyColumnSql = generatePrimaryKeySql(columnDefinitions, primaryKey, tableFullyQualifiedName);
        List<String> otherColumnSqlList = generateOtherColumnSql(columnDefinitions, primaryKey, tableFullyQualifiedName);

        String baseSql = generateFullSql(createTableFirstLine, primaryKeyColumnSql, otherColumnSqlList,
                tableCommentSql, columnComments);

        List<Index> secondaryIndexes = collectSecondaryIndexes(createTable);
        if (secondaryIndexes.isEmpty()) {
            return baseSql;
        }
        StringBuilder builder = new StringBuilder(baseSql);
        builder.append("\n");
        for (Index index : secondaryIndexes) {
            String statement = renderSecondaryIndex(tableFullyQualifiedName, index);
            if (statement != null) {
                builder.append(statement).append("\n");
            }
        }
        return builder.toString();
    }

    private Index resolvePrimaryKeyFromIndex(CreateTable createTable) {
        if (createTable.getIndexes() == null) {
            return null;
        }
        return createTable.getIndexes().stream()
                .filter(idx -> "PRIMARY KEY".equalsIgnoreCase(idx.getType()))
                .findFirst()
                .orElse(null);
    }

    private String extractTableComment(String tableName, List<String> tableOptionsStrings) {
        if (tableOptionsStrings == null) {
            return null;
        }
        int commentIndex = tableOptionsStrings.indexOf("COMMENT");
        if (commentIndex != -1 && commentIndex + 2 < tableOptionsStrings.size()) {
            return String.format("COMMENT ON TABLE %s IS %s;", tableName,
                    tableOptionsStrings.get(commentIndex + 2));
        }
        return null;
    }

    private String generateFullSql(String createTableFirstLine, String primaryKeyColumnSql,
                                   List<String> otherColumnSqlList,
                                   String tableCommentSql, List<String> columnComments) {
        StringBuilder builder = new StringBuilder();
        builder.append(createTableFirstLine).append("\n");
        builder.append("    ").append(primaryKeyColumnSql).append(",\n");

        for (int i = 0; i < otherColumnSqlList.size(); i++) {
            if (i != otherColumnSqlList.size() - 1) {
                builder.append("    ").append(otherColumnSqlList.get(i)).append(",\n");
            } else {
                builder.append("    ").append(otherColumnSqlList.get(i)).append("\n");
            }
        }
        builder.append(");\n");

        if (tableCommentSql != null) {
            builder.append("\n").append(tableCommentSql).append("\n");
        }

        for (String columnComment : columnComments) {
            builder.append(columnComment).append("\n");
        }

        return builder.toString();
    }

    private List<Index> collectSecondaryIndexes(CreateTable createTable) {
        List<Index> indexes = createTable.getIndexes();
        if (indexes == null || indexes.isEmpty()) {
            return List.of();
        }
        List<Index> secondary = new ArrayList<>();
        for (Index index : indexes) {
            String type = index.getType();
            if (type != null && "PRIMARY KEY".equalsIgnoreCase(type.trim())) {
                continue;
            }
            secondary.add(index);
        }
        return secondary;
    }

    private String renderSecondaryIndex(String tableName, Index index) {
        List<String> columns = index.getColumnsNames();
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        boolean unique = index.getType() != null
                && index.getType().toUpperCase(Locale.ROOT).contains("UNIQUE");
        String normalizedTable = normalizeIdentifierForIndexName(tableName);
        String columnSegment = columns.stream()
                .map(this::normalizeIdentifierForIndexName)
                .collect(Collectors.joining("_"));
        String indexName = normalizedTable + "_" + columnSegment + "_idx";
        String columnsClause = String.join(", ", columns);
        return String.format("CREATE %sINDEX %s ON %s (%s);",
                unique ? "UNIQUE " : "",
                indexName,
                tableName,
                columnsClause);
    }

    private String normalizeIdentifierForIndexName(String identifier) {
        if (identifier == null) {
            return "";
        }
        return identifier
                .replace("`", "")
                .replace("\"", "")
                .replace(".", "_")
                .trim()
                .toLowerCase(Locale.ROOT);
    }
}
