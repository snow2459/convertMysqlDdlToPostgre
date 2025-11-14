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
        assertTrue("应保留 0", output.contains("(1, 0)"));
        assertTrue("应保留 1", output.contains("(2, 1)"));
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
    public void shouldKeepTinyintAsInt() throws Exception {
        String sql = ""
                + "CREATE TABLE tiny_bool (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  delete_flag tinyint(1) NOT NULL DEFAULT 0,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("tinyint 应转换为 int", output.contains("delete_flag int"));
        assertTrue("默认值保持为 0", output.contains("DEFAULT 0"));
    }

    @Test
    public void shouldSplitAlterAddColumnComment() throws Exception {
        String sql = ""
                + "ALTER TABLE sys_message_receive\n"
                + "    ADD COLUMN operation_id varchar(100) DEFAULT NULL COMMENT '操作id, 用于异步批量修改时回退操作';\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("ADD COLUMN 中不应包含 COMMENT", !output.contains("ADD COLUMN operation_id varchar(100) DEFAULT NULL COMMENT"));
        assertTrue("应输出 COMMENT ON COLUMN 语句", output.contains("COMMENT ON COLUMN sys_message_receive.operation_id"));
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
