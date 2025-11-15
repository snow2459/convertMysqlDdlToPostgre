package org.example;

import java.util.*;

/**
 * 维护需要强制转换为 boolean 的列清单，支持全局列与表级别列。
 */
public final class BooleanColumnRegistry {

    private static final Set<String> GLOBAL_COLUMNS = new HashSet<>(List.of(
            "is_force_update_password"
    ));

    private static final Map<String, Set<String>> TABLE_SPECIFIC_COLUMNS = new HashMap<>();

    static {
        registerTableColumn("bpm_proc_button", "global_mark");
    }

    private BooleanColumnRegistry() {
    }

    public static boolean isBooleanColumn(String columnName) {
        return isBooleanColumn(null, columnName);
    }

    public static boolean isBooleanColumn(String tableName, String columnName) {
        if (columnName == null) {
            return false;
        }
        String normalizedColumn = normalizeColumn(columnName);
        if (tableName != null) {
            String normalizedTable = normalizeTableName(tableName);
            if (matchesTableSpecific(normalizedTable, normalizedColumn)) {
                return true;
            }
        }
        return GLOBAL_COLUMNS.contains(normalizedColumn);
    }

    public static Set<String> listedColumns() {
        return Collections.unmodifiableSet(GLOBAL_COLUMNS);
    }

    private static boolean matchesTableSpecific(String normalizedTableName, String normalizedColumnName) {
        if (normalizedTableName == null) {
            return false;
        }
        Set<String> columns = TABLE_SPECIFIC_COLUMNS.get(normalizedTableName);
        if (columns != null && columns.contains(normalizedColumnName)) {
            return true;
        }
        String simpleName = simpleTableName(normalizedTableName);
        if (!simpleName.equals(normalizedTableName)) {
            columns = TABLE_SPECIFIC_COLUMNS.get(simpleName);
            return columns != null && columns.contains(normalizedColumnName);
        }
        return false;
    }

    private static void registerTableColumn(String tableName, String columnName) {
        String normalizedTable = normalizeTableName(tableName);
        TABLE_SPECIFIC_COLUMNS
                .computeIfAbsent(normalizedTable, key -> new HashSet<>())
                .add(normalizeColumn(columnName));
    }

    private static String normalizeColumn(String columnName) {
        return columnName
                .replace("\"", "")
                .replace("`", "")
                .trim()
                .toLowerCase(Locale.ROOT);
    }

    private static String normalizeTableName(String tableName) {
        if (tableName == null) {
            return null;
        }
        return tableName
                .replace("\"", "")
                .replace("`", "")
                .trim()
                .toLowerCase(Locale.ROOT);
    }

    private static String simpleTableName(String normalizedTable) {
        if (normalizedTable == null) {
            return null;
        }
        int idx = normalizedTable.lastIndexOf('.');
        if (idx >= 0 && idx + 1 < normalizedTable.length()) {
            return normalizedTable.substring(idx + 1);
        }
        return normalizedTable;
    }
}
