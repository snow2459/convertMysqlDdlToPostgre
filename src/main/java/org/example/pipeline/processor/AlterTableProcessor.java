package org.example.pipeline.processor;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.StatementProcessor;
import org.example.pipeline.converter.CreateTableConverter;

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

    /**
     * 主流程：优先拆解 ADD COLUMN / ADD INDEX，若都未命中则简单替换部分字面量后将原 SQL 透传。
     */
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
        sql = normalize(sql);
        result.appendStatement(sql);
    }

    /**
     * 对常见的 MySQL 语法糖（_utf8mb4/datetime）做轻量替换，减少 PostgreSQL 无法识别的关键字。
     */
    private String normalize(String sql) {
        String normalized = sql.replaceAll("(?i)_utf8mb4'", "'");
        normalized = normalized.replaceAll("(?i)\\bdatetime\\b", "timestamp");
        return normalized;
    }

    /**
     * 拆解 ALTER ... ADD COLUMN 语句：将每个 ColumnDataType 转换为 ALTER TABLE ADD COLUMN + COMMENT。
     */
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
                    stripAfterClause(columnDefinition);
                    if (GeneratedUniqueKeyConverter.tryConvert(alter.getTable().getFullyQualifiedName(), columnDefinition, context, result)) {
                        continue;
                    }
                    handleAddColumnDefinition(alter.getTable().getFullyQualifiedName(), columnDefinition, context, result);
                }
            }
        }
        return handled;
    }

    /**
     * 按需从 ALTER 中抽取 ADD INDEX/ADD KEY，输出为独立的 CREATE INDEX 语句以保持语义一致。
     */
    private boolean processIndexAdditions(Alter alter, ConversionContext context, ConversionResult result) {
        if (!context.getDialectProfile().shouldExtractIndexesFromAlter()) {
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

    /**
     * 对单个新增列执行渲染：克隆列信息、使用 CreateTableConverter 输出最终 SQL，并附带 COMMENT 语句。
     */
    private void handleAddColumnDefinition(String tableName, ColumnDefinition columnDefinition,
                                           ConversionContext context, ConversionResult result) {
        ColumnDefinition cloned = new ColumnDefinition();
        cloned.setColumnName(columnDefinition.getColumnName());
        cloned.setColDataType(columnDefinition.getColDataType());
        cloned.setColumnSpecs(columnDefinition.getColumnSpecs() != null ? new ArrayList<>(columnDefinition.getColumnSpecs()) : null);

        CreateTableConverter converter = context.getDialectProfile().getCreateTableConverter();
        String commentSql = converter.extractSingleColumnComment(tableName, cloned);
        String columnSql = converter.renderColumnDefinition(tableName, cloned);
        String addColumnSql = String.format("ALTER TABLE %s ADD COLUMN %s;", tableName, columnSql);
        result.appendStatement(addColumnSql);

        if (commentSql != null) {
            result.appendStatement(commentSql);
        }
    }

    /**
     * PostgreSQL 不支持 AFTER 子句，提前在列 specs 中移除该信息。
     */
    private void stripAfterClause(ColumnDefinition columnDefinition) {
        List<String> specs = columnDefinition.getColumnSpecs();
        if (specs == null) {
            return;
        }
        for (int i = 0; i < specs.size(); i++) {
            String token = specs.get(i);
            if ("AFTER".equalsIgnoreCase(token)) {
                specs.remove(i);
                if (i < specs.size()) {
                    specs.remove(i);
                }
                break;
            }
        }
    }

    /**
     * 渲染 CREATE INDEX 语句，若索引名称缺失则基于表名和系统 identity 填充占位名。
     */
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
