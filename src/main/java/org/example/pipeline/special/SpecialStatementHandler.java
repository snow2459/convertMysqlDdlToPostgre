package org.example.pipeline.special;

import org.example.pipeline.ConversionContext;
import org.example.pipeline.ConversionResult;

import java.util.List;

/**
 * 处理 JSQLParser 暂不支持的语句，必要时绕过 AST 直接输出目标 SQL。
 */
public final class SpecialStatementHandler {

    private SpecialStatementHandler() {
    }

    public static boolean handle(String rawSql, ConversionContext context, ConversionResult result) {
        List<String> converted = AlterAddIndexConverter.tryConvert(rawSql, context.getDialectProfile());
        if (!converted.isEmpty()) {
            converted.forEach(result::appendStatement);
            return true;
        }
        if (org.example.pipeline.processor.GeneratedUniqueKeyConverter.tryConvertRaw(rawSql, context, result)) {
            return true;
        }
        return false;
    }
}
