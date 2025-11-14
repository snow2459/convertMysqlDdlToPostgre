package org.example;

import java.util.ArrayList;
import java.util.List;

/**
 * 简单 SQL 语句拆分器，按分号切分并忽略字符串/注释内的分号。
 */
public final class SqlStatementSplitter {

    private SqlStatementSplitter() {
    }

    public static List<String> splitStatements(String sqlContent) {
        String normalized = sqlContent.replace("\r\n", "\n");
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBacktick = false;

        for (int i = 0; i < normalized.length(); i++) {
            char c = normalized.charAt(i);

            if (c == '\'' && !inDoubleQuote && !inBacktick && !isEscaped(normalized, i)) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote && !inBacktick && !isEscaped(normalized, i)) {
                inDoubleQuote = !inDoubleQuote;
            } else if (c == '`' && !inSingleQuote && !inDoubleQuote) {
                inBacktick = !inBacktick;
            }

            if (!inSingleQuote && !inDoubleQuote && !inBacktick) {
                if (c == '-' && lookAhead(normalized, i, "--")) {
                    i = skipLineComment(normalized, i + 2);
                    continue;
                } else if (c == '#') {
                    i = skipLineComment(normalized, i + 1);
                    continue;
                } else if (c == '/' && lookAhead(normalized, i, "/*")) {
                    i = skipBlockComment(normalized, i + 2);
                    continue;
                }
            }

            if (c == ';' && !inSingleQuote && !inDoubleQuote && !inBacktick) {
                if (current.length() > 0) {
                    statements.add(current.toString().trim());
                    current.setLength(0);
                }
                continue;
            }

            current.append(c);
        }

        if (current.length() > 0) {
            statements.add(current.toString().trim());
        }
        return statements;
    }

    private static boolean isEscaped(String text, int index) {
        int backslashCount = 0;
        int cursor = index - 1;
        while (cursor >= 0 && text.charAt(cursor) == '\\') {
            backslashCount++;
            cursor--;
        }
        return backslashCount % 2 == 1;
    }

    private static boolean lookAhead(String text, int index, String target) {
        int end = index + target.length();
        if (end > text.length()) {
            return false;
        }
        return text.substring(index, end).equals(target);
    }

    private static int skipLineComment(String text, int index) {
        int cursor = index;
        while (cursor < text.length()) {
            char c = text.charAt(cursor);
            if (c == '\n') {
                return cursor;
            }
            cursor++;
        }
        return cursor - 1;
    }

    private static int skipBlockComment(String text, int index) {
        int cursor = index;
        while (cursor < text.length() - 1) {
            if (text.charAt(cursor) == '*' && text.charAt(cursor + 1) == '/') {
                return cursor + 1;
            }
            cursor++;
        }
        return text.length() - 1;
    }
}
