package org.example;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import org.example.pipeline.DatabaseDialect;
import org.example.pipeline.GaussMySqlDialect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProcessSingleCreateTable {

    public static String process(CreateTable createTable, DatabaseDialect dialect) throws JSQLParserException {
        if (dialect instanceof GaussMySqlDialect) {
            return processForGauss(createTable);
        }
        return processForPostgres(createTable);
    }

    private static String processForGauss(CreateTable createTable) {
        if (createTable.getColumnDefinitions() != null) {
            for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
                if ("datetime".equalsIgnoreCase(columnDefinition.getColDataType().getDataType())) {
                    columnDefinition.getColDataType().setDataType("timestamp");
                }
            }
        }
        String sql = createTable.toString();
        if (!sql.trim().endsWith(";")) {
            sql = sql + ";";
        }
        return sql + "\n";
    }

    private static String processForPostgres(CreateTable createTable) throws JSQLParserException {
        String tableFullyQualifiedName = createTable.getTable().getFullyQualifiedName();
        List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();

        /**
         * 生成目标sql：表注释
         */
        List<String> tableOptionsStrings = createTable.getTableOptionsStrings();
        String tableCommentSql = null;
        if (tableOptionsStrings != null) {
            int commentIndex = tableOptionsStrings.indexOf("COMMENT");
            if (commentIndex != -1 && commentIndex + 2 < tableOptionsStrings.size()) {
                tableCommentSql = String.format("COMMENT ON TABLE %s IS %s;", tableFullyQualifiedName,
                        tableOptionsStrings.get(commentIndex + 2));
            }
        }

        /**
         * 生成目标sql：列注释
         */
        List<String> columnComments = extractColumnCommentSql(tableFullyQualifiedName, columnDefinitions);

        /**
         * 获取主键
         */
        Index primaryKey = resolvePrimaryKey(createTable);
        if (primaryKey == null) {
            throw new RuntimeException("Primary key not found");
        }

        /**
         * 生成目标sql：第一行的建表语句
         */
        String createTableFirstLine = String.format("CREATE TABLE %s (", tableFullyQualifiedName);

        /**
         * 生成目标sql：主键
         */
        String primaryKeyColumnSql = generatePrimaryKeySql(columnDefinitions, primaryKey);
        /**
         * 生成目标sql：除了主键之外的其他列
         */
        List<String> otherColumnSqlList = generateOtherColumnSql(columnDefinitions, primaryKey);


        String fullSql = generateFullSql(createTableFirstLine, primaryKeyColumnSql, otherColumnSqlList,
                tableCommentSql, columnComments);

        return fullSql;
    }

    private static String generateFullSql(String createTableFirstLine, String primaryKeyColumnSql,
                                          List<String> otherColumnSqlList,
                                          String tableCommentSql, List<String> columnComments) {
        StringBuilder builder = new StringBuilder();
        // 建表语句首行
        builder.append(createTableFirstLine)
                .append("\n");
        // 主键 须缩进
        builder.append("    ")
                .append(primaryKeyColumnSql)
                .append(",\n");

        // 每一列 缩进
        for (int i = 0; i < otherColumnSqlList.size(); i++) {
            if (i != otherColumnSqlList.size() - 1) {
                builder.append("    ").append(otherColumnSqlList.get(i)).append(",\n");
            } else {
                builder.append("    ").append(otherColumnSqlList.get(i)).append("\n");
            }
        }
        builder.append(");\n");

        // 表的注释
        if (tableCommentSql != null) {
            builder.append("\n" + tableCommentSql + "\n");
        }

        // 列的注释
        for (String columnComment : columnComments) {
            builder.append(columnComment).append("\n");
        }

        String sql = builder.toString();
        return sql;
    }

    private static List<String> generateOtherColumnSql(List<ColumnDefinition> columnDefinitions, Index primaryKey) {
        String primaryKeyColumnName = primaryKey.getColumnsNames().get(0);

        List<ColumnDefinition> columnDefinitionList = columnDefinitions.stream()
                .filter((ColumnDefinition column) -> !Objects.equals(column.getColumnName(), primaryKeyColumnName))
                .collect(Collectors.toList());

        List<String> sqlList = new ArrayList<String>();
        for (ColumnDefinition columnDefinition : columnDefinitionList) {
            // 列名
            String columnName = columnDefinition.getColumnName();

            // 类型
            String postgreDataType = resolveColumnType(columnDefinition);
            // 处理默认值，将mysql中的默认值转为pg中的默认值，如mysql的CURRENT_TIMESTAMP转为
            List<String> specs = columnDefinition.getColumnSpecs();
            if (specs == null) {
                specs = new ArrayList<>();
                columnDefinition.setColumnSpecs(specs);
            }
            sanitizeColumnSpecs(specs);
            boolean booleanType = "boolean".equalsIgnoreCase(postgreDataType);
            ColumnConstraint constraint = extractColumnConstraint(specs);

            List<String> fragments = new ArrayList<>();
            fragments.add(columnName);
            fragments.add(postgreDataType);
            if (constraint.nullability != null) {
                fragments.add(constraint.nullability);
            }
            if (!constraint.remainingSpecs.isBlank()) {
                fragments.add(constraint.remainingSpecs.trim());
            }
            if (constraint.defaultFragment != null) {
                fragments.add(constraint.defaultFragment);
            }

            sqlList.add(renderColumnFragments(fragments));
        }
        return sqlList;
    }

    public static String renderColumnDefinition(ColumnDefinition columnDefinition) {
        String postgreDataType = resolveColumnType(columnDefinition);
        List<String> specs = columnDefinition.getColumnSpecs();
        if (specs == null) {
            specs = new ArrayList<>();
            columnDefinition.setColumnSpecs(specs);
        }
        sanitizeColumnSpecs(specs);
        ColumnConstraint constraint = extractColumnConstraint(specs);

        List<String> fragments = new ArrayList<>();
        fragments.add(columnDefinition.getColumnName());
        fragments.add(postgreDataType);
        if (constraint.nullability != null) {
            fragments.add(constraint.nullability);
        }
        if (!constraint.remainingSpecs.isBlank()) {
            fragments.add(constraint.remainingSpecs.trim());
        }
        if (constraint.defaultFragment != null) {
            fragments.add(constraint.defaultFragment);
        }
        return renderColumnFragments(fragments);
    }

    public static String extractSingleColumnComment(String tableName, ColumnDefinition columnDefinition) {
        List<String> specs = columnDefinition.getColumnSpecs();
        int commentIndex = getCommentIndex(specs);
        if (commentIndex != -1 && specs != null) {
            int commentStringIndex = commentIndex + 1;
            String commentString = specs.get(commentStringIndex);
            specs.remove(commentStringIndex);
            specs.remove(commentIndex);
            return genCommentSql(tableName, columnDefinition.getColumnName(), commentString);
        }
        return null;
    }

    private static String renderColumnFragments(List<String> fragments) {
        return fragments.stream()
                .filter(part -> part != null && !part.isBlank())
                .collect(Collectors.joining(" "));
    }

    public static boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            double d = Double.parseDouble(strNum);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    private static String generatePrimaryKeySql(List<ColumnDefinition> columnDefinitions, Index primaryKey) {
        // 仅支持单列主键，不支持多列联合主键
        String primaryKeyColumnName = primaryKey.getColumnsNames().get(0);

        ColumnDefinition primaryKeyColumnDefinition = columnDefinitions.stream()
                .filter((ColumnDefinition column) -> column.getColumnName().equals(primaryKeyColumnName))
                .findFirst().orElse(null);
        if (primaryKeyColumnDefinition == null) {
            throw new RuntimeException();
        }
        String primaryKeyType = null;
        String dataType = primaryKeyColumnDefinition.getColDataType().getDataType();
        if (Objects.equals("bigint", dataType)) {
            primaryKeyType = "bigserial";
        } else if (Objects.equals("int", dataType)) {
            primaryKeyType = "serial";
        } else {
            primaryKeyType = resolveColumnType(primaryKeyColumnDefinition);
        }

        String sql = String.format("%s %s PRIMARY KEY", primaryKeyColumnName, primaryKeyType);

        return sql;
    }

    private static List<String> extractColumnCommentSql(String tableFullyQualifiedName, List<ColumnDefinition> columnDefinitions) {
        List<String> columnComments = new ArrayList<>();
        columnDefinitions
                .forEach((ColumnDefinition columnDefinition) -> {
                    List<String> columnSpecStrings = columnDefinition.getColumnSpecs();

                    int commentIndex = getCommentIndex(columnSpecStrings);
                    if (commentIndex != -1 && columnSpecStrings != null) {
                        int commentStringIndex = commentIndex + 1;
                        String commentString = columnSpecStrings.get(commentStringIndex);

                        String commentSql = genCommentSql(tableFullyQualifiedName, columnDefinition.getColumnName(), commentString);
                        columnComments.add(commentSql);

                        columnSpecStrings.remove(commentStringIndex);
                        columnSpecStrings.remove(commentIndex);
                    }
                });

        return columnComments;
    }

    private static int getCommentIndex(List<String> columnSpecStrings) {
        if (columnSpecStrings == null) {
            return -1;
        }
        for (int i = 0; i < columnSpecStrings.size(); i++) {
            if ("COMMENT".equalsIgnoreCase(columnSpecStrings.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private static String genCommentSql(String table, String column, String commentValue) {
        return String.format("COMMENT ON COLUMN %s.%s IS %s;", table, column, commentValue);
    }

    private static Index resolvePrimaryKey(CreateTable createTable) {
        if (createTable.getIndexes() != null) {
            Index index = createTable.getIndexes().stream()
                    .filter((Index idx) -> Objects.equals("PRIMARY KEY", idx.getType()))
                    .findFirst().orElse(null);
            if (index != null) {
                return index;
            }
        }
        List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
        if (columnDefinitions == null) {
            return null;
        }
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            List<String> specs = columnDefinition.getColumnSpecs();
            if (specs == null) {
                continue;
            }
            for (int i = 0; i < specs.size(); i++) {
                String token = specs.get(i);
                if ("PRIMARY".equalsIgnoreCase(token)) {
                    String next = i + 1 < specs.size() ? specs.get(i + 1) : "";
                    if ("KEY".equalsIgnoreCase(next)) {
                        specs.remove(i + 1);
                        specs.remove(i);
                        Index index = new Index();
                        index.setType("PRIMARY KEY");
                        index.setColumnsNames(Collections.singletonList(columnDefinition.getColumnName()));
                        return index;
                    }
                }
            }
        }
        return null;
    }

    private static void sanitizeColumnSpecs(List<String> specs) {
        if (specs == null) {
            return;
        }
        for (int i = 0; i < specs.size(); ) {
            String token = specs.get(i);
            if ("COLLATE".equalsIgnoreCase(token)) {
                specs.remove(i);
                if (i < specs.size()) {
                    specs.remove(i);
                }
                continue;
            }
            if ("CHARACTER".equalsIgnoreCase(token)) {
                specs.remove(i);
                if (i < specs.size() && "SET".equalsIgnoreCase(specs.get(i))) {
                    specs.remove(i);
                }
                if (i < specs.size()) {
                    specs.remove(i);
                }
                continue;
            }
            i++;
        }
    }

    private static String resolveColumnType(ColumnDefinition columnDefinition) {
        String dataType = columnDefinition.getColDataType().getDataType();
        String postgreDataType = DataTypeMapping.lookup(dataType);
        if (postgreDataType == null) {
            System.out.println(columnDefinition.getColDataType().getArgumentsStringList());
            throw new UnsupportedOperationException("mysql dataType not supported yet. " + dataType);
        }
        List<String> argumentsStringList = columnDefinition.getColDataType().getArgumentsStringList();
        String argument = null;
        if (argumentsStringList != null && !argumentsStringList.isEmpty()) {
            if (argumentsStringList.size() == 1) {
                argument = argumentsStringList.get(0);
            } else if (argumentsStringList.size() == 2) {
                argument = argumentsStringList.get(0) + "," + argumentsStringList.get(1);
            }
        }
        if (argument != null && argument.trim().length() != 0) {
            if (!postgreDataType.equalsIgnoreCase("bigint")
                    && !postgreDataType.equalsIgnoreCase("smallint")
                    && !postgreDataType.equalsIgnoreCase("int")
            ) {
                postgreDataType = postgreDataType + "(" + argument + ")";
            }
        }
        return postgreDataType;
    }

    private static ColumnConstraint extractColumnConstraint(List<String> specs) {
        ColumnConstraint constraint = new ColumnConstraint();
        if (specs == null) {
            constraint.remainingSpecs = "";
            return constraint;
        }
        List<String> remaining = new ArrayList<>();
        for (int i = 0; i < specs.size(); ) {
            String token = specs.get(i);
            if ("DEFAULT".equalsIgnoreCase(token)) {
                String defaultValue = (i + 1) < specs.size() ? specs.get(i + 1) : null;
                specs.remove(i);
                if (defaultValue != null) {
                    specs.remove(i);
                    String normalized = normalizeDefaultValue(defaultValue);
                    if ("NULL".equalsIgnoreCase(normalized) && constraint.nullability == null) {
                        constraint.nullability = "NULL";
                    } else {
                        constraint.defaultFragment = "DEFAULT " + normalized;
                    }
                }
                continue;
            }
            if ("NOT".equalsIgnoreCase(token) && (i + 1) < specs.size()
                    && "NULL".equalsIgnoreCase(specs.get(i + 1))) {
                constraint.nullability = "NOT NULL";
                specs.remove(i + 1);
                specs.remove(i);
                continue;
            }
            if ("NULL".equalsIgnoreCase(token)) {
                constraint.nullability = "NULL";
                specs.remove(i);
                continue;
            }
            if ("unsigned".equalsIgnoreCase(token)) {
                specs.remove(i);
                continue;
            }
            if ("ON".equalsIgnoreCase(token) && (i + 2) < specs.size()
                    && "UPDATE".equalsIgnoreCase(specs.get(i + 1))
                    && "CURRENT_TIMESTAMP".equalsIgnoreCase(specs.get(i + 2))) {
                    specs.remove(i + 2);
                    specs.remove(i + 1);
                    specs.remove(i);
                continue;
            }
            remaining.add(token);
            i++;
        }
        constraint.remainingSpecs = String.join(" ", remaining);
        return constraint;
    }

    private static String normalizeDefaultValue(String mysqlDefault) {
        if (mysqlDefault == null) {
            return "NULL";
        }
        String mapped = DefaultValueMapping.lookup(mysqlDefault);
        if (mapped != null) {
            return mapped;
        }
        String trimmed = mysqlDefault.trim();
        return trimmed;
    }

    private static class ColumnConstraint {
        private String nullability;
        private String defaultFragment;
        private String remainingSpecs = "";
    }
}
