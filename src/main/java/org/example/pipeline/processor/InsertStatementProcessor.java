package org.example.pipeline.processor;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import org.example.pipeline.ColumnMetadata;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.SchemaMetadata;
import org.example.pipeline.StatementProcessor;
import org.example.pipeline.TableMetadata;
import org.example.pipeline.dialect.DatabaseDialect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * INSERT 语句转换处理器，当前聚焦 VALUES 场景，未来可扩展 SELECT/SET 形式。
 */
public class InsertStatementProcessor implements StatementProcessor {

    @Override
    public boolean supports(Statement statement) {
        return statement instanceof Insert;
    }

    @Override
    public void process(Statement statement, ConversionContext context, ConversionResult result) {
        Insert insert = (Insert) statement;
        SchemaMetadata schemaMetadata = context.getSchemaMetadata();
        String tableName = insert.getTable().getFullyQualifiedName();
        Optional<TableMetadata> tableMetadata = schemaMetadata.find(tableName);

        try {
            List<String> columnNames = resolveColumnNames(insert, tableMetadata);
            List<List<Expression>> valueRows = extractValueRows(insert.getItemsList());
            if (valueRows.isEmpty()) {
                result.appendStatement(insert.toString());
                return;
            }

            DatabaseDialect dialect = context.getTargetDialect();
            boolean normalizeBoolean = context.getDialectProfile().supportsBooleanLiteralNormalization();
            List<String> renderedRows = renderRows(valueRows, columnNames, tableMetadata.orElse(null), dialect, normalizeBoolean);

            StringBuilder builder = new StringBuilder();
            builder.append("INSERT INTO ")
                    .append(tableName)
                    .append(" (")
                    .append(String.join(", ", columnNames))
                    .append(") VALUES\n");
            builder.append(renderedRows.stream()
                    .map(row -> "    (" + row + ")")
                    .collect(Collectors.joining(",\n")));
            builder.append(";");
            result.appendRaw(builder.append("\n").toString());
        } catch (RuntimeException ex) {
            System.out.println("INSERT 转换失败，保持原语句: " + ex.getMessage());
            result.appendStatement(insert.toString());
        }
    }

    private List<String> resolveColumnNames(Insert insert, Optional<TableMetadata> tableMetadata) {
        if (insert.getColumns() != null && !insert.getColumns().isEmpty()) {
            return insert.getColumns().stream()
                    .map(Column::getColumnName)
                    .collect(Collectors.toList());
        }
        List<ColumnMetadata> columns = tableMetadata
                .map(TableMetadata::getColumnsInDeclarationOrder)
                .orElse(Collections.emptyList());
        if (columns.isEmpty()) {
            throw new IllegalStateException("INSERT 缺少列清单且无法从元数据推断: " + insert);
        }
        return columns.stream()
                .map(ColumnMetadata::getColumnName)
                .collect(Collectors.toList());
    }

    private List<List<Expression>> extractValueRows(ItemsList itemsList) {
        if (itemsList == null) {
            return Collections.emptyList();
        }
        List<List<Expression>> rows = new ArrayList<>();
        if (itemsList instanceof ExpressionList) {
            ExpressionList expressionList = (ExpressionList) itemsList;
            if (containsRowConstructors(expressionList)) {
                for (Expression expression : expressionList.getExpressions()) {
                    RowConstructor rowConstructor = (RowConstructor) expression;
                    rows.add(rowConstructor.getExprList().getExpressions());
                }
            } else {
                rows.add(expressionList.getExpressions());
            }
        } else if (itemsList instanceof MultiExpressionList) {
            for (ExpressionList expressionList : ((MultiExpressionList) itemsList).getExprList()) {
                rows.add(expressionList.getExpressions());
            }
        } else {
            throw new UnsupportedOperationException("当前暂不支持该 INSERT 形式: " + itemsList.getClass().getSimpleName());
        }
        return rows;
    }

    private boolean containsRowConstructors(ExpressionList expressionList) {
        if (expressionList.getExpressions() == null) {
            return false;
        }
        for (Expression expression : expressionList.getExpressions()) {
            if (!(expression instanceof RowConstructor)) {
                return false;
            }
        }
        return true;
    }

    private List<String> renderRows(List<List<Expression>> rows,
                                    List<String> columnNames,
                                    TableMetadata tableMetadata,
                                    DatabaseDialect dialect,
                                    boolean normalizeBoolean) {
        List<String> rendered = new ArrayList<>();
        for (List<Expression> row : rows) {
            if (!columnNames.isEmpty() && columnNames.size() != row.size()) {
                throw new IllegalStateException("列数量与值数量不匹配: " + columnNames + " vs " + row);
            }
            List<String> valueStrings = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                ColumnMetadata columnMetadata = resolveColumnMetadata(columnNames, tableMetadata, i);
                valueStrings.add(renderExpression(row.get(i), columnMetadata, dialect, normalizeBoolean));
            }
            rendered.add(String.join(", ", valueStrings));
        }
        return rendered;
    }

    private ColumnMetadata resolveColumnMetadata(List<String> columnNames, TableMetadata tableMetadata, int index) {
        if (tableMetadata == null || columnNames.isEmpty() || index >= columnNames.size()) {
            return null;
        }
        return tableMetadata.getColumn(columnNames.get(index)).orElse(null);
    }

    private String renderExpression(Expression expression, ColumnMetadata columnMetadata,
                                    DatabaseDialect dialect, boolean normalizeBoolean) {
        boolean binaryColumn = columnMetadata != null && columnMetadata.isBinaryLike();
        if (expression instanceof NullValue) {
            return "NULL";
        }
        if (normalizeBoolean && columnMetadata != null && columnMetadata.isBooleanLike()) {
            Boolean boolValue = extractBooleanValue(expression);
            if (boolValue != null) {
                return dialect.formatBoolean(boolValue);
            }
        }
        if (expression instanceof StringValue) {
            if (binaryColumn) {
                String literal = LiteralSanitizer.removeBinaryPrefix(expression.toString());
                return wrapBinaryLiteral(literal, true);
            }
//            String literal = LiteralSanitizer.removeBinaryPrefix(quoteString(((StringValue) expression).getValue()));
            String literal = LiteralSanitizer.removeBinaryPrefix(expression.toString());
            return wrapBinaryLiteral(literal, false);
        }
        if (expression instanceof LongValue) {
            return wrapBinaryLiteral(String.valueOf(((LongValue) expression).getValue()), binaryColumn);
        }
        if (expression instanceof DoubleValue) {
            return wrapBinaryLiteral(String.valueOf(((DoubleValue) expression).getValue()), binaryColumn);
        }
        String raw = LiteralSanitizer.removeBinaryPrefix(expression.toString());
        return wrapBinaryLiteral(raw, binaryColumn);
    }

    private Boolean extractBooleanValue(Expression expression) {
        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue() != 0;
        }
        if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue() != 0;
        }
        if (expression instanceof StringValue) {
            String value = ((StringValue) expression).getValue();
            String normalized = value.trim().toLowerCase(Locale.ROOT);
            if ("1".equals(normalized) || "true".equals(normalized)) {
                return true;
            }
            if ("0".equals(normalized) || "false".equals(normalized)) {
                return false;
            }
        }
        return null;
    }
    private String quoteString(String value) {
        String escaped = value.replace("'", "''");
        return "'" + escaped + "'";
    }

    private String wrapBinaryLiteral(String literal, boolean binaryColumn) {
        if (!binaryColumn || literal == null) {
            return literal;
        }
        String trimmed = literal.trim();
        if (trimmed.equalsIgnoreCase("NULL")) {
            return literal;
        }
        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (lower.startsWith("convert_to(")) {
            return literal;
        }
        return "convert_to(" + literal + ", 'UTF8')";
    }

}
