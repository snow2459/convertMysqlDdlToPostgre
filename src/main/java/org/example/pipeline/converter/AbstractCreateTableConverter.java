package org.example.pipeline.converter;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.Index;
import org.example.BooleanColumnRegistry;
import org.example.DataTypeMapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 提供列渲染、约束解析等通用能力，具体方言可基于此抽象类实现。
 */
public abstract class AbstractCreateTableConverter implements CreateTableConverter {

    /**
     * 统一封装列定义渲染：复制 AST，规整列属性、类型、默认值与约束，最终拼接出可直接写入 SQL 的单列片段。
     */
    @Override
    public String renderColumnDefinition(String tableName, ColumnDefinition columnDefinition) {
        ColumnDefinition working = cloneColumnDefinition(columnDefinition);
        String postgreDataType = resolveColumnType(working, tableName);
        List<String> specs = working.getColumnSpecs();
        if (specs == null) {
            specs = new ArrayList<>();
            working.setColumnSpecs(specs);
        }
        sanitizeColumnSpecs(specs);
        boolean booleanType = "boolean".equalsIgnoreCase(postgreDataType);
        ColumnConstraint constraint = extractColumnConstraint(specs, booleanType);

        List<String> fragments = new ArrayList<>();
        fragments.add(working.getColumnName());
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

    /**
     * 剥离 ColumnDefinition 内的 COMMENT 片段并转换成单独的 COMMENT ON COLUMN 语句，避免遗留方言差异。
     */
    @Override
    public String extractSingleColumnComment(String tableName, ColumnDefinition columnDefinition) {
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

    /**
     * 遍历建表列定义，将 MySQL COMMENT 子句转换成 PostgreSQL COMMENT 语句列表，并从原 specs 中删除对应 token。
     */
    protected List<String> extractColumnCommentSql(String tableFullyQualifiedName,
                                                   List<ColumnDefinition> columnDefinitions) {
        List<String> columnComments = new ArrayList<>();
        if (columnDefinitions == null) {
            return columnComments;
        }
        columnDefinitions
                .forEach(columnDefinition -> {
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

    /**
     * 优先通过外部 extractor 获取主键，若失败则扫描列内 PRIMARY KEY 声明并构造临时 Index 对象。
     */
    protected Index resolvePrimaryKey(List<ColumnDefinition> columnDefinitions, CreateTablePrimaryKeyExtractor extractor) {
        if (extractor != null) {
            Index idx = extractor.tryResolve();
            if (idx != null) {
                return idx;
            }
        }
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

    private ColumnDefinition cloneColumnDefinition(ColumnDefinition original) {
        ColumnDefinition clone = new ColumnDefinition();
        clone.setColumnName(original.getColumnName());
        clone.setColDataType(original.getColDataType());
        if (original.getColumnSpecs() != null) {
            clone.setColumnSpecs(new ArrayList<>(original.getColumnSpecs()));
        }
        return clone;
    }

    private String renderColumnFragments(List<String> fragments) {
        return fragments.stream()
                .filter(part -> part != null && !part.isBlank())
                .collect(Collectors.joining(" "));
    }

    private int getCommentIndex(List<String> columnSpecStrings) {
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

    private String genCommentSql(String table, String column, String commentValue) {
        return String.format("COMMENT ON COLUMN %s.%s IS %s;", table, column, commentValue);
    }

    protected String generatePrimaryKeySql(List<ColumnDefinition> columnDefinitions, Index primaryKey,
                                           String tableName) {
        if (primaryKey == null) {
            throw new IllegalStateException("Primary key not found");
        }
        String primaryKeyColumnName = primaryKey.getColumnsNames().get(0);

        ColumnDefinition primaryKeyColumnDefinition = columnDefinitions.stream()
                .filter(column -> column.getColumnName().equals(primaryKeyColumnName))
                .findFirst().orElse(null);
        if (primaryKeyColumnDefinition == null) {
            throw new IllegalStateException("Primary key column definition missing");
        }
        String primaryKeyType;
        String dataType = primaryKeyColumnDefinition.getColDataType().getDataType();
        if (Objects.equals("bigint", dataType)) {
            primaryKeyType = "bigserial";
        } else if (Objects.equals("int", dataType)) {
            primaryKeyType = "serial";
        } else {
            primaryKeyType = resolveColumnType(primaryKeyColumnDefinition, tableName);
        }

        return String.format("%s %s PRIMARY KEY", primaryKeyColumnName, primaryKeyType);
    }

    protected List<String> generateOtherColumnSql(List<ColumnDefinition> columnDefinitions, Index primaryKey,
                                                  String tableName) {
        List<String> sqlList = new ArrayList<>();
        if (columnDefinitions == null) {
            return sqlList;
        }
        String primaryKeyColumnName = primaryKey.getColumnsNames().get(0);

        List<ColumnDefinition> columnDefinitionList = columnDefinitions.stream()
                .filter(column -> !Objects.equals(column.getColumnName(), primaryKeyColumnName))
                .collect(Collectors.toList());

        for (ColumnDefinition columnDefinition : columnDefinitionList) {
            sqlList.add(renderColumnDefinition(tableName, columnDefinition));
        }
        return sqlList;
    }

    /**
     * 清理 MySQL 专属的列属性（如 COLLATE/CHARACTER SET），防止不兼容的语法传递到目标 DDL。
     */
    protected void sanitizeColumnSpecs(List<String> specs) {
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

    /**
     * 结合 BooleanColumnRegistry 与 DataTypeMapping 将 MySQL 数据类型映射为目标类型，必要时保留长度或精度定义。
     */
    protected String resolveColumnType(ColumnDefinition columnDefinition, String tableName) {
        String dataType = columnDefinition.getColDataType().getDataType();
        if (isBooleanLike(tableName, columnDefinition)) {
            return "boolean";
        }
        String postgreDataType = DataTypeMapping.lookup(dataType);
        if (postgreDataType == null) {
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
        if (argument != null && !argument.trim().isEmpty()) {
            if (!postgreDataType.equalsIgnoreCase("bigint")
                    && !postgreDataType.equalsIgnoreCase("smallint")
                    && !postgreDataType.equalsIgnoreCase("int")) {
                postgreDataType = postgreDataType + "(" + argument + ")";
            }
        }
        return postgreDataType;
    }

    private boolean isBooleanLike(String tableName, ColumnDefinition columnDefinition) {
        if (BooleanColumnRegistry.isBooleanColumn(tableName, columnDefinition.getColumnName())) {
            return true;
        }
        String dataType = columnDefinition.getColDataType().getDataType();
        if (dataType == null) {
            return false;
        }
        return "boolean".equalsIgnoreCase(dataType);
    }

    /**
     * 将 DEFAULT/NULL/NOT NULL/ON UPDATE/UNSIGNED 等关键信息结构化，便于渲染时按需组合，同时返回剩余未识别的 token。
     */
    protected ColumnConstraint extractColumnConstraint(List<String> specs, boolean booleanType) {
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
                    String normalized = normalizeDefaultValue(defaultValue, booleanType);
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

    /**
     * 默认值正则化：优先查询映射表，不存在则按原始值处理，再根据布尔类型做 TRUE/FALSE 校正。
     */
    protected String normalizeDefaultValue(String mysqlDefault, boolean booleanType) {
        if (mysqlDefault == null) {
            return "NULL";
        }
        String mapped = org.example.DefaultValueMapping.lookup(mysqlDefault);
        if (mapped != null) {
            return adjustBooleanDefault(mapped, booleanType);
        }
        String trimmed = mysqlDefault.trim();
        return adjustBooleanDefault(trimmed, booleanType);
    }

    /**
     * 针对布尔列的默认值做兜底转换：兼容 0/1、't'/'f' 等写法并考虑是否带引号。
     */
    private String adjustBooleanDefault(String value, boolean booleanType) {
        if (!booleanType || value == null) {
            return value;
        }
        String trimmed = value.trim();
        boolean quoted = trimmed.startsWith("'") && trimmed.endsWith("'") && trimmed.length() >= 2;
        String unquoted = quoted ? trimmed.substring(1, trimmed.length() - 1) : trimmed;
        String lower = unquoted.toLowerCase(Locale.ROOT);
        if ("1".equals(lower) || "true".equals(lower) || "t".equals(lower)) {
            return "TRUE";
        }
        if ("0".equals(lower) || "false".equals(lower) || "f".equals(lower)) {
            return "FALSE";
        }
        if ("null".equals(lower)) {
            return "NULL";
        }
        return quoted ? "'" + unquoted + "'" : value;
    }

    /**
     * 承载单列的可空性、默认值与剩余 specs，方便渲染阶段一次性拼接。
     */
    protected static class ColumnConstraint {
        private String nullability;
        private String defaultFragment;
        private String remainingSpecs = "";
    }

    @FunctionalInterface
    protected interface CreateTablePrimaryKeyExtractor {
        Index tryResolve();
    }
}
