package org.example.pipeline.converter.postgres;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ForeignKeyIndex;
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
        List<String> columnSqlList = renderAllColumns(columnDefinitions, tableFullyQualifiedName);
        String primaryKeyConstraint = renderPrimaryKeyConstraint(primaryKey);

        String baseSql = generateFullSql(createTableFirstLine, columnSqlList, primaryKeyConstraint,
                tableCommentSql, columnComments);

        List<Index> secondaryIndexes = collectSecondaryIndexes(createTable);
        List<ForeignKeyIndex> foreignKeys = collectForeignKeys(createTable);
        if (secondaryIndexes.isEmpty() && foreignKeys.isEmpty()) {
            return baseSql;
        }
        StringBuilder builder = new StringBuilder(baseSql);
        boolean appended = false;
        if (!secondaryIndexes.isEmpty()) {
            builder.append("\n");
            for (Index index : secondaryIndexes) {
                String statement = renderSecondaryIndex(tableFullyQualifiedName, index);
                if (statement != null) {
                    builder.append(statement).append("\n");
                }
            }
            appended = true;
        }
        if (!foreignKeys.isEmpty()) {
            if (!appended) {
                builder.append("\n");
            }
            for (ForeignKeyIndex foreignKey : foreignKeys) {
                String statement = renderForeignKeyConstraint(tableFullyQualifiedName, foreignKey);
                if (statement != null) {
                    builder.append(statement).append("\n");
                }
            }
        }
        return builder.toString();
    }

    private List<String> renderAllColumns(List<ColumnDefinition> columnDefinitions, String tableName) {
        List<String> columns = new ArrayList<>();
        if (columnDefinitions == null) {
            return columns;
        }
        for (ColumnDefinition definition : columnDefinitions) {
            columns.add(renderColumnDefinition(tableName, definition));
        }
        return columns;
    }

    private String renderPrimaryKeyConstraint(Index primaryKey) {
        List<String> columns = primaryKey.getColumnsNames();
        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException("Primary key not found");
        }
        List<String> cleaned = columns.stream()
                .map(this::cleanupIdentifier)
                .collect(Collectors.toList());
        return "PRIMARY KEY (" + String.join(", ", cleaned) + ")";
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

    private String generateFullSql(String createTableFirstLine,
                                   List<String> columnSqlList,
                                   String primaryKeyConstraint,
                                   String tableCommentSql, List<String> columnComments) {
        StringBuilder builder = new StringBuilder();
        builder.append(createTableFirstLine).append("\n");
        for (int i = 0; i < columnSqlList.size(); i++) {
            builder.append("    ").append(columnSqlList.get(i));
            builder.append(",\n");
        }
        builder.append("    ").append(primaryKeyConstraint).append("\n");
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
            if (index instanceof ForeignKeyIndex || (type != null && type.equalsIgnoreCase("FOREIGN KEY"))) {
                continue;
            }
            secondary.add(index);
        }
        return secondary;
    }

    private List<ForeignKeyIndex> collectForeignKeys(CreateTable createTable) {
        List<Index> indexes = createTable.getIndexes();
        if (indexes == null || indexes.isEmpty()) {
            return List.of();
        }
        List<ForeignKeyIndex> foreignKeys = new ArrayList<>();
        for (Index index : indexes) {
            if (index instanceof ForeignKeyIndex) {
                foreignKeys.add((ForeignKeyIndex) index);
            }
        }
        return foreignKeys;
    }

    private String renderSecondaryIndex(String tableName, Index index) {
        List<String> columns = index.getColumnsNames();
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        boolean unique = index.getType() != null
                && index.getType().toUpperCase(Locale.ROOT).contains("UNIQUE");
        String indexName = resolveIndexName(tableName, index, columns);
        String columnsClause = columns.stream()
                .map(this::cleanupIdentifier)
                .collect(Collectors.joining(", "));
        return String.format("CREATE %sINDEX %s ON %s (%s);",
                unique ? "UNIQUE " : "",
                indexName,
                tableName,
                columnsClause);
    }

    private String resolveIndexName(String tableName, Index index, List<String> columns) {
        String rawName = index.getName();
        if (rawName != null && !rawName.isBlank()) {
            return sanitizeIndexIdentifier(rawName);
        }
        String normalizedTable = normalizeIdentifierForIndexName(tableName);
        String columnSegment = columns.stream()
                .map(this::normalizeIdentifierForIndexName)
                .collect(Collectors.joining("_"));
        return normalizedTable + "_" + columnSegment + "_idx";
    }

    private String sanitizeIndexIdentifier(String identifier) {
        if (identifier == null) {
            return "";
        }
        return identifier
                .replace("`", "")
                .replace("\"", "")
                .trim();
    }

    private String cleanupIdentifier(String identifier) {
        if (identifier == null) {
            return "";
        }
        return identifier
                .replace("`", "")
                .replace("\"", "")
                .trim();
    }

    private String renderForeignKeyConstraint(String tableName, ForeignKeyIndex foreignKey) {
        List<String> columns = foreignKey.getColumnsNames();
        List<String> referencedColumns = foreignKey.getReferencedColumnNames();
        Table referencedTable = foreignKey.getTable();
        if (columns == null || columns.isEmpty() || referencedTable == null
                || referencedColumns == null || referencedColumns.isEmpty()) {
            return null;
        }
        String constraintName = sanitizeIndexIdentifier(foreignKey.getName());
        String referencingCols = columns.stream()
                .map(this::cleanupIdentifier)
                .collect(Collectors.joining(", "));
        String referencedCols = referencedColumns.stream()
                .map(this::cleanupIdentifier)
                .collect(Collectors.joining(", "));
        String referencedTableName = cleanupIdentifier(referencedTable.getFullyQualifiedName());
        return String.format("ALTER TABLE %s%n    ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);",
                tableName,
                constraintName,
                referencingCols,
                referencedTableName,
                referencedCols);
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
