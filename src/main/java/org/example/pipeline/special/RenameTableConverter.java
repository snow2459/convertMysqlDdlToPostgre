package org.example.pipeline.special;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RenameTableConverter {

    private static final Pattern RENAME_PREFIX = Pattern.compile("^\\s*RENAME\\s+TABLE\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern TO_PATTERN = Pattern.compile("\\s+TO\\s+", Pattern.CASE_INSENSITIVE);

    private RenameTableConverter() {
    }

    public static List<String> tryConvert(String rawSql) {
        if (rawSql == null) {
            return Collections.emptyList();
        }
        Matcher matcher = RENAME_PREFIX.matcher(rawSql);
        if (!matcher.find()) {
            return Collections.emptyList();
        }
        String body = rawSql.substring(matcher.end()).trim();
        if (body.endsWith(";")) {
            body = body.substring(0, body.length() - 1);
        }
        if (body.isEmpty()) {
            return Collections.emptyList();
        }
        String[] pairs = body.split(",");
        List<String> statements = new ArrayList<>();
        for (String pair : pairs) {
            String trimmed = pair.trim();
            Matcher toMatcher = TO_PATTERN.matcher(trimmed);
            if (!toMatcher.find()) {
                return Collections.emptyList();
            }
            String left = trimmed.substring(0, toMatcher.start()).trim();
            String right = trimmed.substring(toMatcher.end()).trim();
            if (left.isEmpty() || right.isEmpty()) {
                return Collections.emptyList();
            }
            statements.add(String.format("ALTER TABLE %s RENAME TO %s;", cleanupIdentifier(left), cleanupIdentifier(right)));
        }
        return statements;
    }

    private static String cleanupIdentifier(String identifier) {
        String cleaned = identifier.trim();
        if (cleaned.startsWith("`") && cleaned.endsWith("`")) {
            cleaned = cleaned.substring(1, cleaned.length() - 1);
        }
        if (cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
            cleaned = cleaned.substring(1, cleaned.length() - 1);
        }
        return cleaned;
    }
}
