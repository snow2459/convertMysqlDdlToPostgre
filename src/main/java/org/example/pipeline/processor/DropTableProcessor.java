package org.example.pipeline.processor;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;
import org.example.ProcessSingleDropTable;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.StatementProcessor;

/**
 * DROP TABLE 转换处理器。
 */
public class DropTableProcessor implements StatementProcessor {

    @Override
    public boolean supports(Statement statement) {
        return statement instanceof Drop;
    }

    @Override
    public void process(Statement statement, ConversionContext context, ConversionResult result) throws JSQLParserException {
        Drop drop = (Drop) statement;
        String sql = ProcessSingleDropTable.process(drop);
        result.appendRaw(sql);
        if (!sql.endsWith("\n")) {
            result.appendRaw("\n");
        }
    }
}
