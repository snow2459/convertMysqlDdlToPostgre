package org.example.pipeline;

/**
 * Gauss 数据库（MySQL 兼容模式）方言。
 * 语义上与 PostgreSQL 接近，这里沿用 PostgreSQL 的布尔与标识符策略，
 * 并通过 {@link org.example.DataTypeMapping} 中的映射将 DATETIME 统一转为 TIMESTAMP。
 */
public class GaussMySqlDialect extends PostgreSqlDialect {

    @Override
    public String getName() {
        return "gauss-mysql";
    }
}
