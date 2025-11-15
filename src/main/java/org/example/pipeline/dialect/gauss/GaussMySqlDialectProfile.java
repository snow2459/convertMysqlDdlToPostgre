package org.example.pipeline.dialect.gauss;

import org.example.pipeline.converter.CreateTableConverter;
import org.example.pipeline.converter.gauss.GaussCreateTableConverter;
import org.example.pipeline.dialect.DialectProfile;

/**
 * Gauss(MySQL) 转换配置，布尔值与索引拆分策略与 PostgreSQL 不同。
 */
public class GaussMySqlDialectProfile extends DialectProfile {

    public GaussMySqlDialectProfile() {
        super(new GaussMySqlDialect(), createConverter());
    }

    private static CreateTableConverter createConverter() {
        return new GaussCreateTableConverter();
    }

    @Override
    public boolean supportsBooleanLiteralNormalization() {
        return false;
    }

    @Override
    public boolean shouldExtractIndexesFromAlter() {
        return false;
    }
}
