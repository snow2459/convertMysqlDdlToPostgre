package org.example;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.drop.Drop;

import java.util.Objects;

/**
 * 兼容旧版流程的 DROP TABLE 处理器：基于 JSQLParser AST 将 MySQL 的 DROP TABLE 语句转成标准 SQL。
 */
public class ProcessSingleDropTable {

    /**
     * 仅处理 DROP TABLE 语句，保留 IF EXISTS 语义，其余类型暂不支持。
     */
    public static String process(Drop drop) throws JSQLParserException {
        String type = drop.getType();
        if (Objects.equals("TABLE", type)) {
            String tableName = drop.getName().toString();
            boolean ifExists = drop.isIfExists();
            String sql = String.format("DROP TABLE %s %s;", ifExists ? "IF EXISTS" : "", tableName);
            return sql;
        }
        throw new UnsupportedOperationException();
    }

}
