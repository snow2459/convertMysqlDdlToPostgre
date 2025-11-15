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
