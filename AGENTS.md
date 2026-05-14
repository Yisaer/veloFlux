# 仓库规范

## 文档与设计背景
- 本项目的功能设计背景与方案沉淀在 `docs/` 目录的 Markdown 文档中；在了解某个功能的背景/约束/方案时，优先查阅对应文档。
- 在 `docs/` 中新增文档时，语法（syntax）类内容需要单独分类/区分，便于后续查找与维护。

## 构建、测试与开发命令
- `make build` — 以 debug 模式编译整个 workspace。
- `make release` — 构建优化后的二进制产物到 `target/release/`。
- `make test` — 运行 workspace 测试以及各 crate 的本地套件（parser、flow、datatypes）。
- `make fmt`, `make clippy` — 执行格式化与 lint（`-D warnings`）。

## 代码风格与命名约定
- 运行 `make fmt`，遵循 Rustfmt 默认风格（4 空格缩进、尾随逗号）。
- 代码、注释与标识符统一使用英文;使用 `snake_case` 命名条目、`CamelCase` 命名类型、SCREAMING_SNAKE_CASE 命名常量。
- 遵守模块边界：parser 不依赖 flow；flow 只消费 `SelectStmt` 的输出（不直接消费原始 `sqlparser::Expr`）。

## 沟通原则
- 与维护者沟通时统一使用中文（需求澄清、方案确认、评审反馈、变更说明等）。

## 编码守则
- 每次写代码前，必须先将代码修改的设计方案与我确认；未经确认不得直接改代码。
- 不用关心 `git status`，交给用户自己操作。
- 开发完成的代码必须通过 `make fmt` 与 `make clippy`；提交/合并前优先用这两个命令自检（clippy 以 `-D warnings` 运行）。
- 任何代码注释以及 `docs/` 下的文档编写均应使用英文。
- 对同一 struct 的同一职责，尽量维护单一方法入口；允许但克制使用 `with_*` 变体，避免因少量参数差异扩散出多个近似方法。需要可选项/扩展点时，优先使用 Options/Config struct、枚举参数，或独立的 builder struct。
- 当需要我们生成 PR title 和 PR description 时，PR title 必须通过 CI 的 `PR Title Lint`（Conventional Commits 规范）。
  - 格式：`<type>(<scope>): <subject>`（`(<scope>)` 可选；breaking change 可用 `!`，例如：`<type>(<scope>)!: <subject>` 或 `<type>!: <subject>`）。
  - `type`/`scope` 使用小写；常用 `type`：`feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert`。
  - 示例：`feat(flow): add ...`、`fix(parser): handle ...`、`docs: update ...`。
  - PR title 和 PR description 必须使用英文撰写。
- 当使用 `gh` 处理 PR review comment 后，如产生本地改动，需要主动提交 commit；提交必须使用 `git commit -s` 保持签名一致，并在 commit message body 中说明处理方式与取舍思路。
- 当 CI 中不熟悉的工具（如 Kani、cargo-deny 等）报错时，必须优先查阅该工具的官方文档理解其工作原理和已知限制，再制定修复方案；禁止仅凭错误信息字面意思猜测 flag 或参数。

## 测试守则
- 不要求每次修改代码后都运行 `make test` 做全量验证；优先运行与改动ni相关的最小测试集合（例如仅运行新增/受影响的单元测试）。
- 在修复 `bench` 相关代码时，永远不要本地运行 `cargo bench`。
- 如需新增单元测试：先向我确认该单元测试的验证用例（case）与预期结果，再进行实现与运行。
- 若修改涉及 `flow` 模块，尤其是 `planner` 相关逻辑，需额外验证 `src/flow/tests/planner/` 下的测试用例。
- `src/flow/tests/planner/` 下的测试用例采用 table-driven 风格：以 SQL 作为主要输入参数，以 `EXPLAIN` 的 JSON 输出作为主要断言依据；同时需要打印 SQL 与 explain 的 pretty string，方便人工阅读与排查。
- `src/flow/tests/pipeline/` 下维护基于 SQL 的输入/输出验证测试，采用 table-driven 风格；新增类似“验证输入/输出”的测试时，优先考虑以新增 case 的方式补充到这里。

## 各模块开发守则
- `parser` 模块：负责将 SQL 解析为 AST；解析结果存放在 `SelectStmt` 中，作为后续模块交互的契约。
- 修改 `parser` 模块相关代码时，可优先阅读 `docs/` 下与语法（syntax）相关的文档。
- `flow` 模块（planner/optimizer）：对于 logical optimize 与 physical optimize 的每一条 rule，优先在 `docs/planner_optimize/logical/` 与 `docs/planner_optimize/physical/` 查找对应设计文档，理解约束与适用边界；再结合 `src/flow/tests/planner/` 的 `EXPLAIN` JSON 断言验证规则效果（如需新增 case，先与维护者确认），确认无误后将结论/注意事项补充回 `AGENTS.md`。
- `processor` 模块：processor 指实际处理流数据的算子；每个 processor 处理完数据后会将数据转交给下一个算子。
- 通道约束：每个 processor 有两类订阅通道 `control channel` 与 `data channel`；从哪个 channel 收到的数据，就只能转发到同类型的 channel（不得混用/串线）。
- `decoder` / `encoder`：`decoder processor` 负责将 `[]byte` 转为 `Collection`/`Tuple`；`encoder` 负责将 `Collection`/`Tuple` 转为 `[]byte`。
- 性能约束：processor 处理 `Collection`/`Tuple` 时尽可能避免值拷贝（优先借用/引用或零拷贝结构）。
- Hot path 约束：对于 processor 中的高频路径（尤其是 `partition key` 相关逻辑），避免使用 `format!("{:?}", key_values)` 这类写法；这类实现通常会同时引入 `Vec<Value>` 分配、`String` 分配和 `Debug` 格式化开销，容易成为明显的 CPU 热点。优先保持结构化 key，并直接使用可 `Hash` / `Eq` 的数据结构作为 key。
