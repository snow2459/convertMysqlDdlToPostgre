package org.example.pipeline.processor;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.DatabaseDialect;
import org.example.pipeline.GaussMySqlDialect;
import org.example.pipeline.StatementProcessor;

/**
 * DELETE 语句处理器。
 */
public class DeleteStatementProcessor implements StatementProcessor {

    @Override
    public boolean supports(Statement statement) {
        return statement instanceof Delete;
    }

    @Override
    public void process(Statement statement, ConversionContext context, ConversionResult result) {
        String sql = statement.toString();
        sql = normalize(sql, context.getTargetDialect());
        result.appendStatement(sql);
    }

    private String normalize(String sql, DatabaseDialect dialect) {
        String normalized = sql.replaceAll("(?i)_utf8mb4'", "'");
        if (dialect instanceof GaussMySqlDialect) {
            return normalized;
        }
        return normalized;
    }
}
