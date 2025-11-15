package org.example.pipeline.dialect.postgres;

import org.example.pipeline.converter.CreateTableConverter;
import org.example.pipeline.converter.postgres.PostgreSqlCreateTableConverter;
import org.example.pipeline.dialect.DialectProfile;

/**
 * PostgreSQL 转换配置。
 */
public class PostgreSqlDialectProfile extends DialectProfile {

    public PostgreSqlDialectProfile() {
        super(new PostgreSqlDialect(), createConverter());
    }

    private static CreateTableConverter createConverter() {
        return new PostgreSqlCreateTableConverter();
    }
}
