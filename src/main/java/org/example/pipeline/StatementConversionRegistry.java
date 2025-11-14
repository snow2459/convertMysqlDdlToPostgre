package org.example.pipeline;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;
import org.example.pipeline.processor.AlterTableProcessor;
import org.example.pipeline.processor.CreateIndexProcessor;
import org.example.pipeline.processor.CreateTableProcessor;
import org.example.pipeline.processor.DeleteStatementProcessor;
import org.example.pipeline.processor.DropTableProcessor;
import org.example.pipeline.processor.InsertStatementProcessor;
import org.example.pipeline.processor.UpdateStatementProcessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 语句处理调度器，可按需注册不同 Processor。
 */
public class StatementConversionRegistry {

    private final List<StatementProcessor> processors = new ArrayList<>();

    public StatementConversionRegistry(List<StatementProcessor> processors) {
        this.processors.addAll(processors);
    }

    public static StatementConversionRegistry defaultRegistry() {
        return new StatementConversionRegistry(Arrays.asList(
                new CreateTableProcessor(),
                new DropTableProcessor(),
                new InsertStatementProcessor(),
                new UpdateStatementProcessor(),
                new DeleteStatementProcessor(),
                new AlterTableProcessor(),
                new CreateIndexProcessor()
        ));
    }

    public void process(Statement statement, ConversionContext context, ConversionResult result) throws JSQLParserException {
        for (StatementProcessor processor : processors) {
            if (processor.supports(statement)) {
                processor.process(statement, context, result);
                return;
            }
        }
        System.out.println("暂未支持的语句，原样输出: " + statement);
        result.appendStatement(statement.toString());
    }
}
