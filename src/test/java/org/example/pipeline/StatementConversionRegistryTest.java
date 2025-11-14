package org.example.pipeline;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.example.pipeline.DialectFactory;
import org.example.pipeline.GaussMySqlDialect;
import org.example.pipeline.PostgreSqlDialect;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StatementConversionRegistryTest {

    @Test
    public void shouldConvertInsertBooleanValues() throws Exception {
        String sql = ""
                + "CREATE TABLE demo (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  flag tinyint(1) NOT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n"
                + "INSERT INTO demo (id, flag) VALUES (1, 0), (2, 1);\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(new PostgreSqlDialect());
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("应将 0 转成 FALSE", output.contains("FALSE"));
        assertTrue("应将 1 转成 TRUE", output.contains("TRUE"));
    }

    @Test
    public void shouldConvertDatetimeToTimestampForGauss() throws Exception {
        String sql = ""
                + "CREATE TABLE demo_gauss (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  created_at datetime DEFAULT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(new GaussMySqlDialect());
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("Gauss 应保持 AUTO_INCREMENT 语法", output.contains("AUTO_INCREMENT"));
        assertTrue("Gauss 应把 datetime 改为 timestamp", output.contains("timestamp"));
    }

    @Test
    public void shouldSupportAlterTableAddColumn() throws Exception {
        String sql = ""
                + "ALTER TABLE demo\n"
                + "    ADD COLUMN created_at datetime default NULL;\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("ALTER 应输出", output.contains("ALTER TABLE demo"));
        assertTrue("PostgreSQL 下 datetime 应转 timestamp", output.contains("timestamp"));
        assertTrue("应移除 _utf8mb4 前缀", !output.contains("_utf8mb4"));
    }

    @Test
    public void shouldSupportCreateIndex() throws Exception {
        String sql = "CREATE INDEX idx_demo ON demo (user_id);\n";
        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("应包含索引语句", output.contains("CREATE INDEX idx_demo ON demo (user_id)"));
    }

    @Test
    public void shouldSupportUpdateStatements() throws Exception {
        String sql = ""
                + "UPDATE sys_dict_data\n"
                + "SET dict_type_code='sys_user_status_type', parent_dict_data_id='-1'\n"
                + "WHERE id='1';\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("应保留 UPDATE 语句", output.contains("UPDATE sys_dict_data"));
    }

    @Test
    public void shouldSupportDeleteStatements() throws Exception {
        String sql = "DELETE FROM sys_menu WHERE name = 'list_test';";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("应保留 DELETE 语句", output.contains("DELETE FROM sys_menu"));
    }

    @Test
    public void shouldHandleInlinePrimaryKeyCreateTable() throws Exception {
        String sql = ""
                + "CREATE TABLE inline_pk (\n"
                + "  id varchar(32) NOT NULL PRIMARY KEY,\n"
                + "  name varchar(20)\n"
                + ");\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("应生成 PRIMARY KEY 行", output.contains("PRIMARY KEY"));
    }
}
