package org.example.pipeline.processor;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.DatabaseDialect;
import org.example.pipeline.GaussMySqlDialect;
import org.example.pipeline.StatementProcessor;

/**
 * UPDATE 语句处理器，确保脚本中 DML 可顺利输出。
 */
public class UpdateStatementProcessor implements StatementProcessor {

    @Override
    public boolean supports(Statement statement) {
        return statement instanceof Update;
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
