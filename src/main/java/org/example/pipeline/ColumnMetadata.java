package org.example.pipeline;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.example.BooleanColumnRegistry;

import java.util.List;
import java.util.Locale;

/**
 * 用于记录列的基本特征，便于 INSERT/UPDATE 转换。
 */
public class ColumnMetadata {

    private final String tableName;
    private final String columnName;
    private final String sourceDataType;
    private final List<String> arguments;

    public ColumnMetadata(String tableName, String columnName, String sourceDataType, List<String> arguments) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.sourceDataType = sourceDataType;
        this.arguments = arguments;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getSourceDataType() {
        return sourceDataType;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public static ColumnMetadata from(String tableName, ColumnDefinition definition) {
        String name = definition.getColumnName();
        String dataType = definition.getColDataType().getDataType();
        List<String> args = definition.getColDataType().getArgumentsStringList();
        return new ColumnMetadata(tableName, name, dataType, args);
    }

    public boolean isBooleanLike() {
        if (BooleanColumnRegistry.isBooleanColumn(tableName, columnName)) {
            return true;
        }
        String type = sourceDataType == null ? "" : sourceDataType.toLowerCase(Locale.ROOT);
        return "boolean".equals(type);
    }
}
