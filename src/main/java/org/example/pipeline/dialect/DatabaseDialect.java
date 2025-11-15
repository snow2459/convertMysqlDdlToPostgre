package org.example.pipeline.dialect;

/**
 * 目标数据库方言定义，用于描述标识符、字面量等基础规则。
 */
public interface DatabaseDialect {

    /**
     * @return 方言名称，主要用于日志与调试。
     */
    String getName();

    /**
     * 按目标方言返回布尔字面量。
     */
    String formatBoolean(boolean value);

    /**
     * 按需为标识符添加转义。简单场景直接返回原值，避免无谓的双引号。
     */
    String maybeQuoteIdentifier(String identifier);
}
