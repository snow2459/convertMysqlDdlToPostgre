package org.example.pipeline;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * 储存转换过程中解析到的表结构信息。
 */
public class SchemaMetadata {

    private final Map<String, TableMetadata> tables = new HashMap<>();

    public void register(TableMetadata tableMetadata) {
        if (tableMetadata == null) {
            return;
        }
        tables.put(normalizeName(tableMetadata.getTableName()), tableMetadata);
    }

    public Optional<TableMetadata> find(String tableName) {
        return Optional.ofNullable(tables.get(normalizeName(tableName)));
    }

    private String normalizeName(String tableName) {
        if (tableName == null) {
            return null;
        }
        return tableName.replace("\"", "").toLowerCase(Locale.ROOT);
    }
}
