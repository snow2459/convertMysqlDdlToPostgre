package org.example.pipeline;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;

/**
 * 不同类型 SQL 语句的处理器。
 */
public interface StatementProcessor {

    boolean supports(Statement statement);

    void process(Statement statement, ConversionContext context, ConversionResult result) throws JSQLParserException;
}
