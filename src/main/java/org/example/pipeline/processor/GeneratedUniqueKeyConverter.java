package org.example.pipeline.processor;

import org.example.pipeline.ColumnMetadata;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.SchemaMetadata;
import org.example.pipeline.TableMetadata;
import org.example.pipeline.dialect.DatabaseDialect;
import org.example.pipeline.dialect.postgres.PostgreSqlDialect;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 将 MySQL 计算列 unique_key 转换为 PostgreSQL 的触发器实现。
 */
public final class GeneratedUniqueKeyConverter {

    private static final Pattern GENERATED_PATTERN =
            Pattern.compile("GENERATED\\s+ALWAYS\\s+AS\\s*\\((.*)\\)\\s*STORED", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern RAW_ALTER_PATTERN =
            Pattern.compile("(?is)ALTER\\s+TABLE\\s+([`\"\\w\\.]+)\\s+ADD\\s+COLUMN\\s+([`\"\\w]+)\\s+(.+?)\\s+GENERATED\\s+ALWAYS\\s+AS\\s*\\((.*)\\)\\s*STORED");
    private static final Pattern COALESCE_PATTERN =
            Pattern.compile("coalesce\\(\\s*`?([a-zA-Z0-9_]+)`?\\s*,", Pattern.CASE_INSENSITIVE);

    private GeneratedUniqueKeyConverter() {
    }

    static boolean tryConvert(String tableName,
                              ColumnDefinition columnDefinition,
                              ConversionContext context,
                              ConversionResult result) {
        if (!isPostgres(context.getDialectProfile().getDialect())) {
            return false;
        }
        if (columnDefinition == null || columnDefinition.getColumnSpecs() == null) {
            return false;
        }
        if (!"unique_key".equalsIgnoreCase(columnDefinition.getColumnName())) {
            return false;
        }
        String definitionSql = columnDefinition.toString();
        Matcher matcher = GENERATED_PATTERN.matcher(definitionSql);
        if (!matcher.find()) {
            return false;
        }
        String expressionBody = matcher.group(1);
        return convertExpression(createTableName(tableName),
                sanitize(columnDefinition.getColumnName()),
                columnDefinition.getColDataType().toString(),
                expressionBody,
                context,
                result);
    }

    public static boolean tryConvertRaw(String rawSql,
                                 ConversionContext context,
                                 ConversionResult result) {
        if (!isPostgres(context.getDialectProfile().getDialect())) {
            return false;
        }
        Matcher matcher = RAW_ALTER_PATTERN.matcher(rawSql);
        if (!matcher.find()) {
            return false;
        }
        String tableName = createTableName(matcher.group(1));
        String columnName = sanitize(matcher.group(2));
        String dataType = matcher.group(3).trim();
        String expressionBody = matcher.group(4);
        return convertExpression(tableName, columnName, dataType, expressionBody, context, result);
    }

    private static boolean convertExpression(String tableName,
                                             String columnName,
                                             String dataType,
                                             String expressionBody,
                                             ConversionContext context,
                                             ConversionResult result) {
        List<String> columns = extractColumns(expressionBody);
        if (columns.isEmpty()) {
            return false;
        }

        SchemaMetadata schemaMetadata = context.getSchemaMetadata();
        Optional<TableMetadata> tableMetadata = schemaMetadata.find(tableName);

        emitPostgresStatements(tableName, columnName, dataType, columns, tableMetadata.orElse(null), result);
        return true;
    }

    private static String createTableName(String raw) {
        return sanitize(raw);
    }

    private static boolean isPostgres(DatabaseDialect dialect) {
        return dialect instanceof PostgreSqlDialect
                || "postgresql".equalsIgnoreCase(dialect.getName());
    }

    private static List<String> extractColumns(String expression) {
        List<String> columns = new ArrayList<>();
        Matcher matcher = COALESCE_PATTERN.matcher(expression);
        while (matcher.find()) {
            columns.add(matcher.group(1));
        }
        return columns;
    }

    private static void emitPostgresStatements(String tableName,
                                               String columnName,
                                               String dataType,
                                               List<String> columns,
                                               TableMetadata tableMetadata,
                                               ConversionResult result) {
        result.appendStatement(String.format("ALTER TABLE %s ADD COLUMN %s %s;", tableName, columnName, dataType));

        String functionName = buildFunctionName(tableName, columnName);
        String triggerName = buildTriggerName(tableName, columnName);
        String inputExpression = buildInputExpression(columns, tableMetadata);
        String functionSql = String.format(
                "CREATE OR REPLACE FUNCTION %s()\n" +
                        "RETURNS TRIGGER AS $$\n" +
                        "DECLARE\n" +
                        "    input_text TEXT;\n" +
                        "BEGIN\n" +
                        "    input_text :=\n%s;\n" +
                        "    NEW.%s := LOWER(SUBSTRING(MD5(input_text) FROM 1 FOR 32));\n" +
                        "    RETURN NEW;\n" +
                        "END;\n" +
                        "$$ LANGUAGE plpgsql;\n",
                functionName,
                inputExpression,
                columnName
        );
        result.appendRaw(functionSql);

        String triggerSql = String.format(
                "CREATE TRIGGER %s\n" +
                        "    BEFORE INSERT OR UPDATE ON %s\n" +
                        "    FOR EACH ROW\n" +
                        "    EXECUTE FUNCTION %s();\n",
                triggerName,
                tableName,
                functionName
        );
        result.appendRaw(triggerSql);

        String indexName = sanitize(tableName).replace(".", "_") + "_" + columnName + "_idx";
        result.appendStatement(String.format("CREATE UNIQUE INDEX %s ON %s (%s);", indexName, tableName, columnName));
    }

    private static String buildInputExpression(List<String> columns, TableMetadata tableMetadata) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            String column = sanitize(columns.get(i));
            ColumnMetadata metadata = tableMetadata == null ? null : tableMetadata.getColumn(column).orElse(null);
            builder.append("        ").append(renderCoalesce(column, metadata));
            if (i != columns.size() - 1) {
                builder.append(" ||\n");
            }
        }
        return builder.toString();
    }

    private static String renderCoalesce(String column, ColumnMetadata metadata) {
        String expression = renderColumnExpression(column, metadata);
        return String.format("COALESCE(%s, '')", expression);
    }

    private static String renderColumnExpression(String column, ColumnMetadata metadata) {
        if (metadata == null || metadata.getSourceDataType() == null) {
            return String.format("NEW.%s::TEXT", column);
        }
        String dataType = metadata.getSourceDataType().toLowerCase(Locale.ROOT);
        if (dataType.contains("char") || dataType.contains("text") || dataType.contains("clob") || dataType.contains("blob")) {
            return String.format("NEW.%s", column);
        }
        if (dataType.contains("date")) {
            return String.format("TO_CHAR(NEW.%s, 'YYYYMMDD')", column);
        }
        return String.format("NEW.%s::TEXT", column);
    }

    private static String buildFunctionName(String tableName, String columnName) {
        return "trg_" + sanitize(tableName).replace(".", "_") + "_" + columnName + "_fn";
    }

    private static String buildTriggerName(String tableName, String columnName) {
        return "trg_" + sanitize(tableName).replace(".", "_") + "_" + columnName;
    }

    private static String sanitize(String identifier) {
        if (identifier == null) {
            return "";
        }
        return identifier.replace("`", "").replace("\"", "").trim();
    }
}
