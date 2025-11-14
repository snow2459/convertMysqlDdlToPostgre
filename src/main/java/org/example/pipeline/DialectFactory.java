package org.example.pipeline;

import java.util.Locale;

/**
 * 简单的方言工厂，根据名称返回对应实现。
 */
public final class DialectFactory {

    private DialectFactory() {
    }

    public static DatabaseDialect fromName(String dialectName) {
        String normalized = dialectName == null ? "" : dialectName.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "gauss":
            case "gauss-mysql":
                return new GaussMySqlDialect();
            case "postgres":
            case "postgresql":
            default:
                return new PostgreSqlDialect();
        }
    }
}
