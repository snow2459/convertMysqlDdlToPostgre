package org.example.pipeline.special;

import org.example.pipeline.ConversionResult;
import org.example.pipeline.DatabaseDialect;

import java.util.List;

/**
 * 处理 JSQLParser 暂不支持的语句，必要时绕过 AST 直接输出目标 SQL。
 */
public final class SpecialStatementHandler {

    private SpecialStatementHandler() {
    }

    public static boolean handle(String rawSql, DatabaseDialect dialect, ConversionResult result) {
        List<String> converted = AlterAddIndexConverter.tryConvert(rawSql, dialect);
        if (!converted.isEmpty()) {
            converted.forEach(result::appendStatement);
            return true;
        }
        return false;
    }
}
