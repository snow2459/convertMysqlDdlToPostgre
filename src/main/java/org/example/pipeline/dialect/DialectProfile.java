package org.example.pipeline.dialect;

import org.example.pipeline.converter.CreateTableConverter;

/**
 * 描述特定方言下的转换策略与特性，用于驱动各处理器行为。
 */
public abstract class DialectProfile {

    private final DatabaseDialect dialect;
    private final CreateTableConverter createTableConverter;

    protected DialectProfile(DatabaseDialect dialect, CreateTableConverter converter) {
        this.dialect = dialect;
        this.createTableConverter = converter;
    }

    public DatabaseDialect getDialect() {
        return dialect;
    }

    public CreateTableConverter getCreateTableConverter() {
        return createTableConverter;
    }

    /**
     * 是否需要在 INSERT/UPDATE 中对布尔列进行 TRUE/FALSE 格式化。
     */
    public boolean supportsBooleanLiteralNormalization() {
        return true;
    }

    /**
     * ALTER TABLE ... ADD INDEX 是否需要拆分为独立 CREATE INDEX 语句。
     */
    public boolean shouldExtractIndexesFromAlter() {
        return true;
    }
}
