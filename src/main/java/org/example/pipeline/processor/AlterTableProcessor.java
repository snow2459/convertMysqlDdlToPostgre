package org.example.pipeline.processor;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.DatabaseDialect;
import org.example.pipeline.GaussMySqlDialect;
import org.example.pipeline.StatementProcessor;

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
}
