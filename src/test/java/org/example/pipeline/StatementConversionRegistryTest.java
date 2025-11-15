package org.example.pipeline;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.example.pipeline.DialectFactory;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StatementConversionRegistryTest {

    @Test
    public void shouldConvertInsertBooleanValues() throws Exception {
        String sql = ""
                + "CREATE TABLE demo (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  is_force_update_password tinyint(1) NOT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n"
                + "INSERT INTO demo (id, is_force_update_password) VALUES (1, 0), (2, 1);\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
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
        ConversionContext context = new ConversionContext(DialectFactory.fromName("gauss"));
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
    public void shouldKeepLineBreaksForGauss() throws Exception {
        String sql = ""
                + "CREATE TABLE demo_gauss (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  created_at datetime DEFAULT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("gauss"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("Gauss 建表首行应带换行缩进", output.contains("CREATE TABLE demo_gauss (\n    id int"));
        assertTrue("Gauss 建表列之间应换行", output.contains(",\n    created_at timestamp"));
        assertTrue("Gauss 建表结尾应独立换行", output.contains("\n);\n"));
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
    public void shouldConvertWhitelistedTinyintToBoolean() throws Exception {
        String sql = ""
                + "CREATE TABLE tiny_bool (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  is_force_update_password tinyint(1) NOT NULL DEFAULT 0,\n"
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
        assertTrue("白名单 tinyint 应转换为 boolean", output.contains("is_force_update_password boolean"));
        assertTrue("默认值应转换为 FALSE", output.contains("DEFAULT FALSE"));
    }

    @Test
    public void shouldKeepTinyintAsSmallintWhenNotWhitelisted() throws Exception {
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
        assertTrue("非白名单 tinyint 应保留下推到 smallint", output.contains("delete_flag smallint"));
        assertTrue("默认值应保持数字", output.contains("DEFAULT 0"));
    }

    @Test
    public void shouldConvertTableSpecificIntColumnToBoolean() throws Exception {
        String sql = ""
                + "CREATE TABLE bpm_proc_button (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  global_mark int NOT NULL DEFAULT 0,\n"
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
        assertTrue("表级配置列应转换为 boolean", output.contains("global_mark boolean"));
        assertTrue("表级配置默认值应转换为 FALSE", output.contains("DEFAULT FALSE"));
    }

    @Test
    public void shouldNotConvertSameColumnNameInOtherTable() throws Exception {
        String sql = ""
                + "CREATE TABLE other_button (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  global_mark int NOT NULL DEFAULT 0,\n"
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
        assertTrue("非指定表的列不应转换", output.contains("global_mark int"));
        assertTrue("默认值仍保持数字", output.contains("DEFAULT 0"));
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
    public void shouldConvertUpdateBooleanAssignments() throws Exception {
        String sql = ""
                + "CREATE TABLE bool_table (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  is_force_update_password tinyint(1) NOT NULL DEFAULT 0,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n"
                + "UPDATE bool_table SET is_force_update_password = 1 WHERE id = 1;\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("UPDATE 应将 1 转 TRUE", output.contains("SET is_force_update_password = TRUE"));
    }

    @Test
    public void shouldConvertInsertValuesForTableSpecificBooleanColumns() throws Exception {
        String sql = ""
                + "CREATE TABLE bpm_proc_button (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  global_mark int NOT NULL DEFAULT 0,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n"
                + "INSERT INTO bpm_proc_button (id, global_mark) VALUES (1, 0), (2, 1);\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("INSERT 应将 0 转 FALSE", output.contains("(1, FALSE)"));
        assertTrue("INSERT 应将 1 转 TRUE", output.contains("(2, TRUE)"));
    }

    @Test
    public void shouldConvertUpdateForTableSpecificBooleanColumns() throws Exception {
        String sql = ""
                + "CREATE TABLE bpm_proc_button (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  global_mark int NOT NULL DEFAULT 0,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n"
                + "UPDATE bpm_proc_button SET global_mark = 1 WHERE id = 1;\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("表级列 UPDATE 应转换为 TRUE", output.contains("SET global_mark = TRUE"));
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
