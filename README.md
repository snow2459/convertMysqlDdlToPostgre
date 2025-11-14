# convertMysqlDdlToPostgre
convert mysql ddl to postgre sql syntax

## 方言切换

运行程序时可通过 JVM 系统属性 `target.dialect` 指定输出目标：

```bash
mvn exec:java -Dexec.mainClass=org.example.App -Dtarget.dialect=postgresql
mvn exec:java -Dexec.mainClass=org.example.App -Dtarget.dialect=gauss
```

- `postgresql`（默认）: 输出 PostgreSQL 语法。
- `gauss`: Gauss 数据库 MySQL 兼容模式，保持 MySQL 语法，仅将 `datetime` 字段转为 `timestamp`。

如需新增方言，请参考 `docs/dialect_guide.md`。
