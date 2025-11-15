package org.example.pipeline;

import org.example.pipeline.dialect.DialectProfile;
import org.example.pipeline.dialect.DatabaseDialect;

/**
 * 转换上下文，贯穿整个脚本解析过程，记录方言与元数据等信息。
 */
public class ConversionContext {

    private final DialectProfile dialectProfile;
    private final SchemaMetadata schemaMetadata;

    public ConversionContext(DialectProfile dialectProfile) {
        this.dialectProfile = dialectProfile;
        this.schemaMetadata = new SchemaMetadata();
    }

    public DatabaseDialect getTargetDialect() {
        return dialectProfile.getDialect();
    }

    public DialectProfile getDialectProfile() {
        return dialectProfile;
    }

    public SchemaMetadata getSchemaMetadata() {
        return schemaMetadata;
    }
}
