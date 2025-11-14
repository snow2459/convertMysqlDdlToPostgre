package org.example.pipeline;

/**
 * 转换上下文，贯穿整个脚本解析过程，记录方言与元数据等信息。
 */
public class ConversionContext {

    private final DatabaseDialect targetDialect;
    private final SchemaMetadata schemaMetadata;

    public ConversionContext(DatabaseDialect targetDialect) {
        this.targetDialect = targetDialect;
        this.schemaMetadata = new SchemaMetadata();
    }

    public DatabaseDialect getTargetDialect() {
        return targetDialect;
    }

    public SchemaMetadata getSchemaMetadata() {
        return schemaMetadata;
    }
}
