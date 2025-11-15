package org.example.pipeline.dialect.gauss;

import org.example.pipeline.dialect.DatabaseDialect;
import org.example.pipeline.dialect.postgres.PostgreSqlDialect;

/**
 * Gauss 数据库（MySQL 兼容模式）方言，沿用 PostgreSQL 的布尔与转义策略。
 */
public class GaussMySqlDialect extends PostgreSqlDialect implements DatabaseDialect {

    @Override
    public String getName() {
        return "gauss-mysql";
    }
}
