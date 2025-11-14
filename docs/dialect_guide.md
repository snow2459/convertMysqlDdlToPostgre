# 新增数据库方言指引

本文介绍如何基于当前的转换管线扩展新的目标数据库方言。

## 1. 基础概念

- **DatabaseDialect**：定义布尔字面量、标识符转义等基础规则。
- **ConversionContext**：承载目标方言与 `SchemaMetadata`，供各处理器使用。
- **StatementProcessor**：负责某一类 SQL 的语法树转换，当前包含 CREATE/DROP/INSERT。
- **StatementConversionRegistry**：根据语句类型分派到对应的 Processor。

## 2. 新增方言步骤

1. **实现方言类**
   - 在 `org.example.pipeline` 包下创建 `YourDialect.java` 并实现 `DatabaseDialect`。
   - 返回独一无二的 `getName()`；根据目标库规范覆盖 `formatBoolean`、`maybeQuoteIdentifier`。
   - 如需自定义类型/默认值映射，可在 `ProcessSingleCreateTable` 或专用映射表中引入方言判断。

2. **注册方言**
   - 在 `DialectFactory#fromName` 中添加方言名称映射，便于通过 `-Dtarget.dialect` 启用。

3. **扩展 Processor（可选）**
   - 若目标库需要额外的语句转换逻辑，可在 `StatementConversionRegistry.defaultRegistry()` 中按需添加 Processor，或自定义一个新的 Registry 构造方法。

4. **编写测试**
   - 在 `src/test/java/org/example/pipeline` 下新增针对性测试，验证主要 DDL/DML 转换行为。
   - 推荐使用真实 SQL 片段（脱敏），并覆盖类型映射、默认值、布尔/字符串等边界。

## 3. 运行与验证

```bash
mvn exec:java -Dexec.mainClass=org.example.App -Dtarget.dialect=your-dialect
```

生成的 `target.sql` 与日志中的“当前目标方言”应匹配。若需批量对比，可把多份 SQL 放入 `src/main/resources/`，反复执行验证。

## 4. 常见扩展点

- **类型映射**：根据方言自定义 `DataTypeMapping`，或引入 `TypeMappingRegistry` 按方言注入。
- **默认值/函数**：在 `DefaultValueMapping` 中引入方言分支，确保类似 `CURRENT_TIMESTAMP(6)` 的函数可落地。
- **注释/格式化**：如目标方言需要特殊注释语法，可在 `ConversionResult` 增加策略。

通过以上步骤即可快速为新的数据库方言提供转换能力。若遇到 AST 不支持的语句，建议先在 Processor 中输出原语句并记录日志，再逐步增强解析逻辑。***
