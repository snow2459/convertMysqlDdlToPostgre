package org.example.pipeline.processor;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import org.example.pipeline.ColumnMetadata;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.SchemaMetadata;
import org.example.pipeline.StatementProcessor;
import org.example.pipeline.TableMetadata;
import org.example.pipeline.dialect.DatabaseDialect;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * UPDATE 语句处理器，确保脚本中 DML 可顺利输出。
 */
public class UpdateStatementProcessor implements StatementProcessor {

    private static final String BOOL_TRUE_TOKEN = "__PG_BOOL_TRUE__";
    private static final String BOOL_FALSE_TOKEN = "__PG_BOOL_FALSE__";

    @Override
    public boolean supports(Statement statement) {
        return statement instanceof Update;
    }

    @Override
    public void process(Statement statement, ConversionContext context, ConversionResult result) {
        Update update = (Update) statement;
        DatabaseDialect dialect = context.getTargetDialect();
        SchemaMetadata schemaMetadata = context.getSchemaMetadata();
        Optional<TableMetadata> tableMetadata = schemaMetadata.find(update.getTable().getFullyQualifiedName());
        boolean normalizeBoolean = context.getDialectProfile().supportsBooleanLiteralNormalization();
        if (tableMetadata.isPresent() && normalizeBoolean) {
            applyBooleanAssignments(update, tableMetadata.get());
        }

        String sql = update.toString();
        sql = normalize(sql, dialect, normalizeBoolean);
        result.appendStatement(sql);
    }

    private String normalize(String sql, DatabaseDialect dialect, boolean normalizeBoolean) {
        String normalized = sql.replaceAll("(?i)_utf8mb4'", "'");
        if (normalizeBoolean) {
            normalized = normalized
                    .replace("'" + BOOL_TRUE_TOKEN + "'", dialect.formatBoolean(true))
                    .replace("'" + BOOL_FALSE_TOKEN + "'", dialect.formatBoolean(false));
        }
        return normalized;
    }

    private void applyBooleanAssignments(Update update, TableMetadata tableMetadata) {
        if (update.getUpdateSets() == null) {
            return;
        }
        for (UpdateSet updateSet : update.getUpdateSets()) {
            List<Column> columns = updateSet.getColumns();
            List<Expression> expressions = updateSet.getExpressions();
            if (columns == null || expressions == null) {
                continue;
            }
            for (int i = 0; i < columns.size() && i < expressions.size(); i++) {
                Column column = columns.get(i);
                ColumnMetadata columnMetadata = tableMetadata.getColumn(column.getColumnName()).orElse(null);
                if (columnMetadata != null && columnMetadata.isBooleanLike()) {
                    Boolean boolValue = extractBooleanValue(expressions.get(i));
                    if (boolValue != null) {
                        expressions.set(i, new StringValue(boolValue ? BOOL_TRUE_TOKEN : BOOL_FALSE_TOKEN));
                    }
                }
            }
        }
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
}
