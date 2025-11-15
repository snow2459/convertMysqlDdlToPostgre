package org.example.pipeline;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * 记录单张表的列元数据（保持顺序），为 INSERT 转换提供支撑。
 */
public class TableMetadata {

    private final String tableName;
    private final Map<String, ColumnMetadata> columnsByName = new LinkedHashMap<>();

    public TableMetadata(String tableName) {
        this.tableName = normalizeName(tableName);
    }

    public static TableMetadata from(CreateTable createTable) {
        TableMetadata metadata = new TableMetadata(createTable.getTable().getFullyQualifiedName());
        if (createTable.getColumnDefinitions() != null) {
            for (ColumnDefinition definition : createTable.getColumnDefinitions()) {
                metadata.addColumn(ColumnMetadata.from(metadata.getTableName(), definition));
            }
        }
        return metadata;
    }

    public void addColumn(ColumnMetadata columnMetadata) {
        columnsByName.put(normalizeName(columnMetadata.getColumnName()), columnMetadata);
    }

    public String getTableName() {
        return tableName;
    }

    public Optional<ColumnMetadata> getColumn(String columnName) {
        if (columnName == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(columnsByName.get(normalizeName(columnName)));
    }

    public List<ColumnMetadata> getColumnsInDeclarationOrder() {
        return new ArrayList<>(columnsByName.values());
    }

    public static String normalizeName(String name) {
        if (name == null) {
            return null;
        }
        return name.replace("\"", "").toLowerCase(Locale.ROOT);
    }
}
