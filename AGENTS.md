# Repository Guidelines

## 项目结构与模块组织
- `src/main/java/org/example` 包含 `App` 启动器与 `ProcessSingleCreateTable/DropTable`、`DataTypeMapping` 等转换核心，遵循“解析→类型映射→目标 SQL”链路，保持每个类专注单一职责。
- `src/main/resources/source-mysql-ddl.txt` 用于存放 MySQL DDL 示例，可替换为待转换文件；运行后结果输出到仓库根目录的 `target.sql`，利于比对差异。
- `src/test/java` 按包名镜像主代码，`AppTest` 用于回归基本流程；新增测试请以功能域命名子包并保留同名辅助 SQL 片段以便复用。
- `pom.xml` 管理依赖与插件，`target/` 为 Maven 输出目录，请勿提交；公共脚本或样例可放在根目录并配套 README 说明。

## 构建、测试与开发命令
- `mvn clean verify`：清理并执行编译、单元测试，是最小交付前置条件；缺陷修复需附上运行截图或日志摘要。
- `mvn exec:java -Dexec.mainClass=org.example.App`：在开发态运行转换器，默认读取资源文件并写入 `target.sql`，必要时可追加 `-Dexec.args="classpath:/custom.sql"` 指定不同资源。
- `mvn -DskipTests package`：在需要快速验证打包逻辑但测试已在其他流水线跑完时使用，应在 PR 描述中说明测试托管位置。
- 使用 IntelliJ IDEA 可通过“Run App”配置直接指向 `org.example.App` 主类，便于断点调试与观察 AST 结构。

## 代码风格与命名约定
- 统一使用 Java 17 语法、4 空格缩进，`UpperCamelCase` 命名类、`lowerCamelCase` 命名方法与变量；字段常量请使用 `SCREAMING_SNAKE_CASE`。
- 映射配置类（如 `DataTypeMapping`）保持“输入方言→输出方言”字典式命名，便于扩展与差异化审查。
- 引入新依赖前更新 `pom.xml` 注释说明用途，并保持 import 按包名排序，同时执行 `mvn -q dependency:tree` 确认未引入冲突。
- 推荐在提交前运行 IDE 的“Reformat Code”与“Optimize Imports”，确保 diff 聚焦业务逻辑，必要时附上 UML 草图链接。

## 测试指引
- 使用 Maven Surefire + JUnit 5；测试类命名 `*Test`，方法使用 Given_When_Then 语义，断言消息写明原始列类型与期望类型。
- 覆盖重点应落在类型映射与默认值转换边界（NULL、AUTO_INCREMENT、时间戳），同时验证 Drop 语句降级逻辑。
- 执行 `mvn test -Dtest=ProcessSingleCreateTableTest` 可单独调试；补充断言时注意使用多行 SQL 样例并提交到 `src/test/resources`.
- 目标覆盖率建议保持在 80% 以上；对 bug 修复必须附带回归测试，以防未来合并回归。

## 提交与 Pull Request 规范
- 历史提交以简短祈使句（如“init version”）为主，建议升级为 `<scope>: <动词>`，示例：`converter: 支持 bit 类型`，方便快速定位变更意图。
- PR 描述需包含：问题背景、方案要点、验证方式（附命令输出或生成的 `target.sql` 摘要），如涉及 DDL 示例请脱敏并标注来源库版本。
- 关联 Issue 时使用关键词 `Closes #id`；多分支协作建议以 `feature/<模块>` 命名并保持 rebase，确保主干保持线性并利于回滚。
- 大型 PR 请拆分为可独立审查的批次，并在清单中列出影响模块、数据迁移步骤及潜在风险，方便评审人复现。

## 架构与扩展策略
- 核心流程基于 JSQLParser，将 SQL 抽象语法树映射为 PostgreSQL 语句；扩展新方言时优先新增映射类，避免在 `App` 中堆叠条件。
- 数据类型与默认值策略建议通过配置映射表驱动，提交前运行回归用例并附带新增映射示例，防止破坏既有转换规则。
- 当需要处理索引、外键等高级语句时，可在 `org.example` 下创建子包 `ddl.index`、`ddl.constraint`，确保包级依赖树清晰。
- 面向长远扩展，建议抽象 `ConversionPipeline` 接口，允许注入多阶段处理器（预清洗、语义校验、格式化），以适配不同数据库版本。

## 安全与配置提示
- `source-mysql-ddl.txt` 不应包含生产凭证或用户数据，提交前执行 `git update-index --assume-unchanged` 保护本地敏感文件。
- 本工具不连接数据库，仅处理文件；若需自动下载 DDL，请通过外部脚本生成并存储到 `src/main/resources/import` 目录，运行完毕后清理。
- CI 环境建议将 `target.sql` 作为构建工件上传以供审计，必要时启用 `md5sum target.sql` 记录完整性。
- 若需要共享样例，请使用 `samples/<日期>-<库名>.sql` 命名并记录脱敏策略，避免团队成员误用生产数据。
