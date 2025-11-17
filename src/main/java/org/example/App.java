package org.example;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;
import org.example.pipeline.DialectFactory;
import org.example.pipeline.StatementConversionRegistry;
import org.example.pipeline.special.SpecialStatementHandler;
import org.example.pipeline.dialect.DialectProfile;
import org.example.pipeline.dialect.DatabaseDialect;
import org.example.pipeline.SqlPreprocessor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 命令行入口：串联“读取→预处理→解析→方言转换→写出”完整流程。
 * <p>默认从 classpath 下的 {@code source-mysql-ddl.txt} 读取 MySQL DDL，
 * 根据 {@code target.dialect} 系统属性选择目标方言（缺省为 PostgreSQL），
 * 最后将转换结果写入工程根目录的 {@code target.sql}。</p>
 */
public class App {

    /**
     * 入口流程：
     * <ol>
     *     <li>读取 DDL，并做基础清洗（移除反引号等 MySQL 特有语法）。</li>
     *     <li>根据目标方言构建 {@link ConversionContext}、注册默认处理器。</li>
     *     <li>调用 {@link SqlStatementSplitter} 将原始文本拆成独立语句。</li>
     *     <li>若命中特殊语句（如 RENAME TABLE）则由 {@link SpecialStatementHandler} 直接处理。</li>
     *     <li>其余语句交给 JSQLParser 解析为 AST，再由对应 Processor 产出目标 SQL。</li>
     *     <li>聚合所有结果写回 {@code target.sql}，方便与源 DDL 对比。</li>
     * </ol>
     */
    public static void main(String[] args) throws JSQLParserException, IOException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = contextClassLoader.getResourceAsStream("source-mysql-ddl.txt");
        if (inputStream == null) {
            throw new RuntimeException();
        }
        String sqlContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        sqlContent = sqlContent.replaceAll("`","");

        String targetDialectName = System.getProperty("target.dialect", "postgresql");
//        String targetDialectName = System.getProperty("target.dialect", "gauss");
        DialectProfile targetProfile = DialectFactory.fromName(targetDialectName);
        DatabaseDialect targetDialect = targetProfile.getDialect();
        System.out.println("当前目标方言: " + targetDialect.getName());

        ConversionContext conversionContext = new ConversionContext(targetProfile);
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult conversionResult = new ConversionResult();

        List<String> statements = SqlStatementSplitter.splitStatements(sqlContent);
        if (statements.isEmpty()) {
            System.out.println("未解析到可用 SQL 语句");
            return;
        }
        for (String originalSql : statements) {
            String rawSql = SqlPreprocessor.sanitize(originalSql);
            if (rawSql == null || rawSql.trim().isEmpty()) {
                continue;
            }
            if (SpecialStatementHandler.handle(rawSql, conversionContext, conversionResult)) {
                continue;
            }
            try {
                Statement statement = CCJSqlParserUtil.parse(rawSql);
                registry.process(statement, conversionContext, conversionResult);
            } catch (Exception ex) {
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
