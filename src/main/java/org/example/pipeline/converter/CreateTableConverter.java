package org.example.pipeline.converter;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

/**
 * 针对 CREATE TABLE 语句的方言化转换器，负责输出目标 SQL 并为列/注释渲染提供统一入口。
 */
public interface CreateTableConverter {

    /**
     * 将 AST 转换为目标 SQL。
     */
    String convert(CreateTable createTable) throws JSQLParserException;

    /**
     * 渲染单列定义，供 ALTER TABLE ADD COLUMN 等场景复用。
     */
    String renderColumnDefinition(String tableName, ColumnDefinition columnDefinition);

    /**
     * 提取列注释 SQL，若不存在则返回 null。
     */
    String extractSingleColumnComment(String tableName, ColumnDefinition columnDefinition);
}
