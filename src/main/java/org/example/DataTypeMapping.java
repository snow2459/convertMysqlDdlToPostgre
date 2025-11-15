package org.example;

import java.util.HashMap;
import java.util.Locale;

/**
 * 参考
 * https://dev.mysql.com/doc/workbench/en/wb-migration-database-postgresql-typemapping.html
 * https://dev.mysql.com/doc/refman/8.0/en/data-types.html
 * https://www.postgresql.org/docs/11/datatype-numeric.html#DATATYPE-INT
 */
public class DataTypeMapping {
    public static final HashMap<String, String> MYSQL_TYPE_TO_POSTGRE_TYPE
            = new HashMap<String, String>();

    static {
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("bigint", "bigint");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("int", "int");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("tinyint", "smallint");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("smallint", "smallint");

        MYSQL_TYPE_TO_POSTGRE_TYPE.put("varchar", "varchar");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("char", "char");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("text", "text");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("tinytext", "text");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("mediumtext", "text");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("longtext", "text");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("blob", "bytea");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("mediumblob", "bytea");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("longblob", "bytea");

        MYSQL_TYPE_TO_POSTGRE_TYPE.put("datetime", "timestamp");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("timestamp", "timestamp");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("date", "date");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("time", "time");

        MYSQL_TYPE_TO_POSTGRE_TYPE.put("decimal", "numeric");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("double", "DOUBLE PRECISION");
        MYSQL_TYPE_TO_POSTGRE_TYPE.put("float", "real");
    }

    public static String lookup(String mysqlType) {
        if (mysqlType == null) {
            return null;
        }
        String direct = MYSQL_TYPE_TO_POSTGRE_TYPE.get(mysqlType);
        if (direct != null) {
            return direct;
        }
        return MYSQL_TYPE_TO_POSTGRE_TYPE.get(mysqlType.toLowerCase(Locale.ROOT));
    }
}
