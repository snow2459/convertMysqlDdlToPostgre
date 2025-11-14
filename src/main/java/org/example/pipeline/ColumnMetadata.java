package org.example.pipeline;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.List;
import java.util.Locale;

/**
 * 用于记录列的基本特征，便于 INSERT 转换。
 */
public class ColumnMetadata {

    private final String columnName;
    private final String sourceDataType;
    private final List<String> arguments;

    public ColumnMetadata(String columnName, String sourceDataType, List<String> arguments) {
        this.columnName = columnName;
        this.sourceDataType = sourceDataType;
        this.arguments = arguments;
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

    public static ColumnMetadata from(ColumnDefinition definition) {
        String name = definition.getColumnName();
        String dataType = definition.getColDataType().getDataType();
        List<String> args = definition.getColDataType().getArgumentsStringList();
        return new ColumnMetadata(name, dataType, args);
    }

    public boolean isBooleanLike() {
        String type = sourceDataType == null ? "" : sourceDataType.toLowerCase(Locale.ROOT);
        if ("boolean".equals(type)) {
            return true;
        }
        if ("tinyint".equals(type)) {
            return hasSingleLengthArgument();
        }
        if ("bit".equals(type)) {
            return hasSingleLengthArgument();
        }
        return false;
    }

    private boolean hasSingleLengthArgument() {
        if (arguments == null || arguments.isEmpty()) {
            return true;
        }
        return "1".equals(arguments.get(0));
    }
}
