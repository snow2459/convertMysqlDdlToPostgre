package org.example.pipeline.processor;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.StatementProcessor;

/**
 * CREATE INDEX/CREATE UNIQUE INDEX 语句处理器。
 * MySQL 与 PostgreSQL 语法兼容度高，此处以格式化输出为主。
 */
public class CreateIndexProcessor implements StatementProcessor {

    @Override
    public boolean supports(Statement statement) {
        return statement instanceof CreateIndex;
    }

    @Override
    public void process(Statement statement, ConversionContext context, ConversionResult result) {
        String sql = statement.toString();
        result.appendStatement(sql);
    }
}
