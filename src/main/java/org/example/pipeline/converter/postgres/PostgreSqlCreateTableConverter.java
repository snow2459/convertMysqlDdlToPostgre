package org.example.pipeline.converter.postgres;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import org.example.pipeline.converter.AbstractCreateTableConverter;

import java.util.List;

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

        return generateFullSql(createTableFirstLine, primaryKeyColumnSql, otherColumnSqlList,
                tableCommentSql, columnComments);
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
}
