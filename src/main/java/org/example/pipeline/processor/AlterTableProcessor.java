package org.example.pipeline.processor;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.example.ProcessSingleCreateTable;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.DatabaseDialect;
import org.example.pipeline.GaussMySqlDialect;
import org.example.pipeline.StatementProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * ALTER TABLE 语句处理器，当前聚焦列新增/索引等语句的透传与必要的方言替换。
 */
public class AlterTableProcessor implements StatementProcessor {

    @Override
    public boolean supports(Statement statement) {
        return statement instanceof Alter;
    }

    @Override
    public void process(Statement statement, ConversionContext context, ConversionResult result) {
        Alter alter = (Alter) statement;
        if (processColumnAdditions(alter, context, result)) {
            return;
        }
        if (processIndexAdditions(alter, context, result)) {
            return;
        }
        String sql = alter.toString();
        sql = normalize(sql, context.getTargetDialect());
        result.appendStatement(sql);
    }

    private String normalize(String sql, DatabaseDialect dialect) {
        String normalized = sql.replaceAll("(?i)_utf8mb4'", "'");
        normalized = normalized.replaceAll("(?i)\\bdatetime\\b", "timestamp");
        if (dialect instanceof GaussMySqlDialect) {
            // Gauss 仅需处理 datetime -> timestamp，保持其余 MySQL 语法
            return normalized;
        }
        return normalized;
    }

    private boolean processColumnAdditions(Alter alter, ConversionContext context, ConversionResult result) {
        if (alter.getAlterExpressions() == null || alter.getAlterExpressions().isEmpty()) {
            return false;
        }
        boolean handled = false;
        for (AlterExpression expression : alter.getAlterExpressions()) {
            if (expression.getOperation() == AlterOperation.ADD && expression.getColDataTypeList() != null) {
                handled = true;
                for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
                    ColumnDefinition columnDefinition = new ColumnDefinition();
                    columnDefinition.setColumnName(columnDataType.getColumnName());
                    columnDefinition.setColDataType(columnDataType.getColDataType());
                    columnDefinition.setColumnSpecs(columnDataType.getColumnSpecs());
                    handleAddColumnDefinition(alter.getTable().getFullyQualifiedName(), columnDefinition, result);
                }
            }
        }
        return handled;
    }

    private boolean processIndexAdditions(Alter alter, ConversionContext context, ConversionResult result) {
        if (context.getTargetDialect() instanceof GaussMySqlDialect) {
            return false;
        }
        if (alter.getAlterExpressions() == null) {
            return false;
        }
        boolean handled = false;
        for (AlterExpression expression : alter.getAlterExpressions()) {
            if (expression.getOperation() == AlterOperation.ADD && expression.getIndex() != null) {
                handled = true;
                String createSql = renderCreateIndex(alter.getTable().getFullyQualifiedName(), expression);
                result.appendStatement(createSql);
            }
        }
        return handled;
    }

    private void handleAddColumnDefinition(String tableName, ColumnDefinition columnDefinition, ConversionResult result) {
        ColumnDefinition cloned = new ColumnDefinition();
        cloned.setColumnName(columnDefinition.getColumnName());
        cloned.setColDataType(columnDefinition.getColDataType());
        cloned.setColumnSpecs(columnDefinition.getColumnSpecs() != null ? new ArrayList<>(columnDefinition.getColumnSpecs()) : null);

        String commentSql = ProcessSingleCreateTable.extractSingleColumnComment(tableName, cloned);
        String columnSql = ProcessSingleCreateTable.renderColumnDefinition(cloned);
        String addColumnSql = String.format("ALTER TABLE %s ADD COLUMN %s;", tableName, columnSql);
        result.appendStatement(addColumnSql);

        if (commentSql != null) {
            result.appendStatement(commentSql);
        }
    }

    private String renderCreateIndex(String tableName, AlterExpression expression) {
        var index = expression.getIndex();
        String indexName = index.getName();
        if (indexName == null || indexName.isBlank()) {
            indexName = tableName + "_idx_" + System.identityHashCode(index);
        }
        boolean unique = index.getType() != null && index.getType().toUpperCase(Locale.ROOT).contains("UNIQUE");
        List<String> columns = index.getColumnsNames();
        String columnsClause = columns == null ? "" : String.join(", ", columns);
        return String.format("CREATE %sINDEX %s ON %s (%s);",
                unique ? "UNIQUE " : "",
                indexName,
                tableName,
                columnsClause);
    }
}
