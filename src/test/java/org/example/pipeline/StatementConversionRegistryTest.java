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
    public void shouldRenderForeignKeyConstraint() throws Exception {
        String sql = ""
                + "CREATE TABLE act_ge_bytearray (\n"
                + "  id varchar(64) NOT NULL,\n"
                + "  deployment_id varchar(64) DEFAULT NULL,\n"
                + "  PRIMARY KEY (id),\n"
                + "  KEY ACT_FK_BYTEARR_DEPL (deployment_id),\n"
                + "  CONSTRAINT ACT_FK_BYTEARR_DEPL FOREIGN KEY (deployment_id) REFERENCES act_re_deployment (id)\n"
                + ");\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("应保留外键索引", output.contains("CREATE INDEX ACT_FK_BYTEARR_DEPL ON act_ge_bytearray (deployment_id);"));
        assertTrue("应生成外键约束", output.contains("ALTER TABLE act_ge_bytearray\n    ADD CONSTRAINT ACT_FK_BYTEARR_DEPL FOREIGN KEY (deployment_id) REFERENCES act_re_deployment (id);"));
    }

    @Test
    public void shouldGenerateIndexesForPostgres() throws Exception {
        String sql = ""
                + "CREATE TABLE idx_demo (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  code varchar(20) NOT NULL,\n"
                + "  tenant_id int DEFAULT NULL,\n"
                + "  PRIMARY KEY (id),\n"
                + "  UNIQUE KEY uk_code (code),\n"
                + "  KEY idx_tenant (tenant_id)\n"
                + ");\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("应生成唯一索引并沿用原名", output.contains("CREATE UNIQUE INDEX uk_code ON idx_demo (code);"));
        assertTrue("应生成普通索引并沿用原名", output.contains("CREATE INDEX idx_tenant ON idx_demo (tenant_id);"));
    }

    @Test
    public void shouldWrapBlobInsertWithConvertTo() throws Exception {
        String sql = ""
                + "CREATE TABLE blob_demo (\n"
                + "  id int NOT NULL AUTO_INCREMENT,\n"
                + "  payload longblob,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n"
                + "INSERT INTO blob_demo (id, payload) VALUES (1, 'binary-content');\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("BLOB 插入应包装 convert_to", output.contains("convert_to('binary-content', 'UTF8')"));
    }

    @Test
    public void shouldRemoveAfterClauseForAlterAddColumn() throws Exception {
        String sql = ""
                + "ALTER TABLE bpm_de_model ADD COLUMN app_url varchar(255) NULL after url;\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("ALTER 输出应不含 after 关键字", !output.toLowerCase().contains("after"));
        assertTrue("应生成标准 ADD COLUMN 语句", output.contains("ADD COLUMN app_url varchar(255) NULL;"));
    }

    @Test
    public void shouldConvertGeneratedUniqueKeyColumn() throws Exception {
        String sql = ""
                + "CREATE TABLE analysis_event_daily_aggregation (\n"
                + "  id bigint,\n"
                + "  user_id varchar(64),\n"
                + "  user_name varchar(64),\n"
                + "  user_account varchar(64),\n"
                + "  org_id varchar(64),\n"
                + "  org_name varchar(64),\n"
                + "  org_level int,\n"
                + "  app_id varchar(64),\n"
                + "  create_date date,\n"
                + "  page_url text,\n"
                + "  page_title text,\n"
                + "  system_code varchar(64),\n"
                + "  user_ip varchar(64),\n"
                + "  event_id varchar(64),\n"
                + "  event_text text,\n"
                + "  event_type varchar(32),\n"
                + "  event_p1 varchar(64),\n"
                + "  event_p2 varchar(64),\n"
                + "  event_p3 varchar(64),\n"
                + "  event_p4 varchar(64),\n"
                + "  event_p5 varchar(64),\n"
                + "  event_p6 varchar(64),\n"
                + "  event_p7 varchar(64),\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n"
                + "ALTER TABLE analysis_event_daily_aggregation\n"
                + "    ADD COLUMN unique_key CHAR(32) GENERATED ALWAYS AS (\n"
                + "        MD5(CONCAT(\n"
                + "                coalesce(`user_id`, ''),\n"
                + "                coalesce(`user_name`, ''),\n"
                + "                coalesce(`user_account`, ''),\n"
                + "                coalesce(`org_id`, ''),\n"
                + "                coalesce(`org_name`, ''),\n"
                + "                coalesce(`org_level`, ''),\n"
                + "                coalesce(`app_id`, ''),\n"
                + "                coalesce(`create_date`, ''),\n"
                + "                coalesce(`page_url`, ''),\n"
                + "                coalesce(`page_title`, ''),\n"
                + "                coalesce(`system_code`, ''),\n"
                + "                coalesce(`user_ip`, ''),\n"
                + "                coalesce(`event_id`, ''),\n"
                + "                coalesce(`event_text`, ''),\n"
                + "                coalesce(`event_type`, ''),\n"
                + "                coalesce(`event_p1`, ''),\n"
                + "                coalesce(`event_p2`, ''),\n"
                + "                coalesce(`event_p3`, ''),\n"
                + "                coalesce(`event_p4`, ''),\n"
                + "                coalesce(`event_p5`, ''),\n"
                + "                coalesce(`event_p6`, ''),\n"
                + "                coalesce(`event_p7`, '')\n"
                + "            ))\n"
                + "        ) STORED;\n";

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        ConversionContext context = new ConversionContext(DialectFactory.fromName("postgresql"));
        StatementConversionRegistry registry = StatementConversionRegistry.defaultRegistry();
        ConversionResult result = new ConversionResult();

        for (Statement statement : statements.getStatements()) {
            registry.process(statement, context, result);
        }

        String output = result.asSql();
        assertTrue("应添加物理列", output.contains("ALTER TABLE analysis_event_daily_aggregation ADD COLUMN unique_key CHAR (32);"));
        assertTrue("应生成触发器函数", output.contains("CREATE OR REPLACE FUNCTION trg_analysis_event_daily_aggregation_unique_key_fn()"));
        assertTrue("应创建触发器", output.contains("CREATE TRIGGER trg_analysis_event_daily_aggregation_unique_key"));
        assertTrue("应创建唯一索引", output.contains("CREATE UNIQUE INDEX analysis_event_daily_aggregation_unique_key_idx"));
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
