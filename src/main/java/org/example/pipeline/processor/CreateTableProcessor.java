package org.example.pipeline.processor;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.example.ProcessSingleCreateTable;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.StatementProcessor;
import org.example.pipeline.TableMetadata;

/**
 * CREATE TABLE 转换处理器：沿用原有逻辑并同步记录元数据。
 */
public class CreateTableProcessor implements StatementProcessor {

    @Override
    public boolean supports(Statement statement) {
        return statement instanceof CreateTable;
    }

    @Override
    public void process(Statement statement, ConversionContext context, ConversionResult result) throws JSQLParserException {
        CreateTable createTable = (CreateTable) statement;
        String sql = ProcessSingleCreateTable.process(createTable, context.getTargetDialect());
        appendWithNewline(result, sql);
        context.getSchemaMetadata().register(TableMetadata.from(createTable));
    }

    private void appendWithNewline(ConversionResult result, String sql) {
        result.appendRaw(sql);
        if (!sql.endsWith("\n")) {
            result.appendRaw("\n");
        }
    }
}
