package org.example.pipeline.converter.gauss;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.example.pipeline.converter.AbstractCreateTableConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * Gauss(MySQL 模式) 建表转换器，保留大部分 MySQL 原生语法，仅对 DATETIME 做兜底。
 */
public class GaussCreateTableConverter extends AbstractCreateTableConverter {

    @Override
    public String convert(CreateTable createTable) throws JSQLParserException {
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
        return formatGaussCreateTable(sql);
    }

    private String formatGaussCreateTable(String sql) {
        int leftParenthesisIndex = sql.indexOf('(');
        if (leftParenthesisIndex == -1) {
            return ensureTrailingNewline(sql);
        }
        int rightParenthesisIndex = findMatchingRightParenthesis(sql, leftParenthesisIndex);
        if (rightParenthesisIndex == -1) {
            return ensureTrailingNewline(sql);
        }
        List<String> definitions = splitTopLevelSegments(sql.substring(leftParenthesisIndex + 1, rightParenthesisIndex));
        if (definitions.isEmpty()) {
            return ensureTrailingNewline(sql);
        }
        StringBuilder builder = new StringBuilder();
        builder.append(sql, 0, leftParenthesisIndex + 1).append("\n");
        for (int i = 0; i < definitions.size(); i++) {
            String definition = definitions.get(i).trim();
            if (definition.isEmpty()) {
                continue;
            }
            builder.append("    ").append(definition);
            if (i != definitions.size() - 1) {
                builder.append(",\n");
            } else {
                builder.append("\n");
            }
        }
        builder.append(sql.substring(rightParenthesisIndex));
        return ensureTrailingNewline(builder.toString());
    }

    private int findMatchingRightParenthesis(String sql, int startIndex) {
        if (startIndex < 0 || startIndex >= sql.length()) {
            return -1;
        }
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = startIndex; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '\'' && !inDoubleQuote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inSingleQuote = !inSingleQuote;
                continue;
            }
            if (ch == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }
            if (inSingleQuote || inDoubleQuote) {
                continue;
            }
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private List<String> splitTopLevelSegments(String body) {
        List<String> segments = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < body.length(); i++) {
            char ch = body.charAt(i);
            if (ch == '\'' && !inDoubleQuote) {
                if (i + 1 < body.length() && body.charAt(i + 1) == '\'') {
                    current.append(ch).append(body.charAt(i + 1));
                    i++;
                    continue;
                }
                inSingleQuote = !inSingleQuote;
                current.append(ch);
                continue;
            }
            if (ch == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                current.append(ch);
                continue;
            }
            if (!inSingleQuote && !inDoubleQuote) {
                if (ch == '(') {
                    depth++;
                } else if (ch == ')' && depth > 0) {
                    depth--;
                } else if (ch == ',' && depth == 0) {
                    String segment = current.toString().trim();
                    if (!segment.isEmpty()) {
                        segments.add(segment);
                    }
                    current.setLength(0);
                    continue;
                }
            }
            current.append(ch);
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) {
            segments.add(last);
        }
        return segments;
    }

    private String ensureTrailingNewline(String sql) {
        if (!sql.endsWith("\n")) {
            return sql + "\n";
        }
        return sql;
    }
}
