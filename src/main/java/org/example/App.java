package org.example;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.DatabaseDialect;
import org.example.pipeline.DialectFactory;
import org.example.pipeline.StatementConversionRegistry;
import org.example.pipeline.special.SpecialStatementHandler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) throws JSQLParserException, IOException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = contextClassLoader.getResourceAsStream("source-mysql-ddl.txt");
        if (inputStream == null) {
            throw new RuntimeException();
        }
        String sqlContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        sqlContent = sqlContent.replaceAll("`","");

        String targetDialectName = System.getProperty("target.dialect", "postgresql");
        DatabaseDialect targetDialect = DialectFactory.fromName(targetDialectName);
        System.out.println("当前目标方言: " + targetDialect.getName());

        ConversionContext conversionContext = new ConversionContext(targetDialect);
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult conversionResult = new ConversionResult();

        List<String> statements = SqlStatementSplitter.splitStatements(sqlContent);
        if (statements.isEmpty()) {
            System.out.println("未解析到可用 SQL 语句");
            return;
        }
        for (String rawSql : statements) {
            try {
                Statement statement = CCJSqlParserUtil.parse(rawSql);
                registry.process(statement, conversionContext, conversionResult);
            } catch (JSQLParserException ex) {
                if (SpecialStatementHandler.handle(rawSql, targetDialect, conversionResult)) {
                    continue;
                }
                System.out.println("解析失败，原样输出: " + abbreviate(rawSql) + "，原因: " + ex.getMessage());
                conversionResult.appendStatement(rawSql);
            }
        }

        File destFile = new File(System.getProperty("user.dir"), "target.sql");
        FileUtils.writeStringToFile(destFile, conversionResult.asSql(), StandardCharsets.UTF_8);
        System.out.println("file saved to :" + destFile.getAbsolutePath());

    }

    private static String abbreviate(String sql) {
        String singleLine = sql.replaceAll("\\s+", " ").trim();
        if (singleLine.length() <= 120) {
            return singleLine;
        }
        return singleLine.substring(0, 117) + "...";
    }
}
