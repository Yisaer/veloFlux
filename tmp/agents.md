## SynapseFlow LLM Agent 设计文档（中文）

本文档描述如何为 SynapseFlow 设计一个 LLM agent：将用户自然语言需求转换为 **SynapseFlow 真正支持** 的 SQL，并通过调用项目自身（parser/planner/explain）形成“可验证、可自纠错”的闭环。

> 说明：仓库 `docs/` 下的文档按约定应使用英文；本文件放在 `tmp/` 下用于中文讨论与草稿沉淀。

### 背景与痛点

用户往往：

- 不熟悉 SynapseFlow 的 SQL 方言（parser 支持哪些语法、哪些不支持）。
- 不知道有哪些函数可用、函数含义/参数/返回类型/限制是什么。
- 不知道 SQL 会被 planner lower 成哪些算子（operators/processors），因此无法判断语义是否正确。

一个通用 LLM 在不接触源码/能力边界的情况下，很容易“编造”不存在的函数或不支持的语法，导致用户体验差。

### 目标

- NL → 生成 **可运行/可解释** 的 SynapseFlow SQL。
- agent 能自动验证与修正：`validate` + `explain` 作为裁判。
- agent 不仅输出 SQL，还能解释关键函数/算子含义，让用户逐步学会使用该项目。

### 核心原则：把“源码理解”产品化

不要指望 agent 运行时去读一堆 match 分支猜能力；应把隐式能力提升为显式元数据（meta），做到：

- 单一事实源（single source of truth）
- 可导出 JSON（给 agent 用）
- 可渲染 Markdown（给人看）
- 与实现足够贴近，避免漂移

### 统一视角：编译链

可以把系统视为一条编译链：

`自然语言意图 → SQL（表面语言）→ AST（parser）→ Logical Plan（planner）→ Operators/Processors（执行）`

agent 最可靠的策略不是“先语法后算子”或“先算子后语法”二选一，而是：

- **先语义（Plan-first）**：把意图归一成关系代数草图（project/filter/aggregate/sort/limit…）
- **再语法（Syntax-constrained）**：用项目支持的语法子集把语义渲染成 SQL
- **用 explain 校验**：以 EXPLAIN 的 JSON/pretty 输出为最终裁判，确认 plan 结构符合预期

### MVP 需要落地的 4 类元信息（Catalog）

#### 1) Streams / Schema（基于 StreamDefinition）

agent 必须能回答：

- 系统里有哪些 stream？
- 某个 stream 有哪些列？列类型是什么？是否可空？
- 是否存在 event-time / watermark / primary key 等会影响 planner 的语义约束？

建议字段：

- `name`：SQL 中引用的稳定名称
- `description`：stream 含义（1–3 句）
- `columns[]`：`{ name, type, nullable, description }`
- 可选：`event_time_column`、`primary_key`、`watermark`、`tags`、`aliases`

这里的“事实来源”应以**运行时 catalog** 为准（通常由 `StreamDefinition` 构建而来）。agent 应把 stream 当作可查询的数据对象，通过 `list_streams/describe_stream` 等接口获取，而不是依赖理解 manager 内部如何创建/运行 stream。

#### 2) Functions（函数注册表 + 语义）

agent 只能从函数表中选择函数，避免“编函数名”。

建议字段：

- `name`、`aliases`
- `args[]`：`{ name, type, optional, variadic }`
- `return_type`
- `description`
- `constraints`：null 处理/时区/确定性/单位/范围等
- `examples[]`：可复制的 SQL 片段

#### 3) Syntax Features（parser 支持面 + 明确限制）

把“支持/限制/替代写法”写清楚（优先维护 allowlist：只列出当前支持的语法构造；未出现的构造默认不支持，后续遇到幻觉严重的点再补显式 `unsupported` 也可）。

建议按“语法构造树（construct tree）”组织：

- Statements / clauses / windowing / expressions 分组
- 每个构造包含 agent 友好的字段：用途（purpose）、语义（semantics）、允许位置（placement）、约束（constraints）、语法模板（syntax）、示例（examples）
- 子构造用 `children` 显式表达层级关系（例如 `window.state` → `window.state.over_partition_by`），不要依赖字符串拆分猜层级

#### 4) Operators / Plan Nodes（planner 输出的节点语义）

agent 需要能解释 plan、并自动校验 plan 是否符合意图。

建议字段：

- `kind/name`：与 EXPLAIN JSON 里的节点类型对齐（稳定）
- `semantics`：一句话 + 详细描述
- `inputs_outputs`：是否改变 schema/行数、排序/partition 要求
- `traits`：是否 stateful / time-aware / order-sensitive 等
- `constraints`：通道约束等
- `typical_sql_patterns`：常见触发 SQL pattern（桥接 SQL ↔ plan）
- `examples`：canonical SQL（优先复用 planner tests 中已有 SQL）

### SQL ↔ Plan 的连接方式：以 EXPLAIN 为契约

不要靠猜 lowering 规则：

- 确保 EXPLAIN JSON 中 `kind/type`、children、关键 expr/schema 字段稳定可用
- 将 `src/flow/tests/planner/` 的 table-driven 用例当作“真实映射样本库”
- 在 operator 元数据里沉淀常用 SQL 模板与示例

### Agent 需要的工具接口（建议）

最小工具集（建议稳定、结构化）：

- `list_streams()`：列出 stream 名 + 简述（用于实体对齐/消歧）
- `describe_stream(name)`：返回 schema（用于确认列存在与类型）
- `list_functions()`：列出函数（含 kind/签名摘要），用于避免“编函数名”
- `describe_function(name)`：返回函数详情（签名/限制/示例），用于按 kind 约束生成
- `capabilities/syntax`：返回语法构造树（给 agent 做语法约束）
- `capabilities/functions/operators`：返回 functions/operators（给 agent 做约束）
- `validate_sql(sql)`：parser + 语义校验（结构化错误）
- `explain_sql(sql)`：planner 输出 JSON + pretty（用于验证算子/结构）

后续可选：

- `sample(stream, n)`：抽样数据理解值域（需考虑安全/隐私）
- `run/preview(sql)`：端到端验证（成本更高）

### 结构化错误（自纠错关键）

建议错误包含：

- `code`：稳定错误码
- `message`：简短说明
- `span`：位置（行列或 offset）
- `hints`：修复建议
- `candidates`：未知列/函数的候选项

### 推荐的 agent 工作流

1) 解析用户意图，识别 stream/列/聚合/过滤/排序等要素  
2) 先查 schema：`describe_stream()`，确认列存在与类型可用  
3) 生成语义草图（关系代数 pipeline）  
4) 用 catalog 约束生成 SQL（只用支持的语法和函数）  
5) `validate_sql()` 纠错；`explain_sql()` 校验 plan 结构与节点类型  
6) 输出 SQL + 简要解释 + explain 摘要；必要时向用户提澄清问题

何时必须澄清：

- stream 名不明确或不存在
- 列缺失/类型不匹配（例如对字符串做 `+ 1`）
- 用户需求涉及不支持的语法特性（给替代写法或问是否接受简化）

### 在源码中如何填写元信息（建议做法）

建议以“显式注册表”为核心：

- Streams：由 `StreamDefinition` / 运行时 registry 导出统一 schema
- Functions：实现旁边写 `FunctionDef` 元数据，并集中注册 `all_functions()`
- Syntax：维护 `SyntaxFeature`/limitations 列表（支持与不支持都写清）
- Operators：为每个 plan node/operator 写 `OperatorDef`，并附带典型 SQL pattern 与示例

同一份元数据应同时用于：

- `describe --json`（给 agent 使用）
- `docs/` 文档生成（给人使用）

### 交付顺序（建议）

1) 先补齐 streams/functions/syntax/operators 四类 catalog（字段先最小化）  
2) 做 `describe/validate/explain`（CLI 或库 API 均可）  
3) 再实现最小 agent：按“查 catalog → 生成 → validate/explain → 修正”闭环跑通  
4) 再逐步增强：结构化错误、模板库、更多算子/函数覆盖、可选端到端数据验证

---

## PR 提交流程约定

- PR 目标远端只有 `git remote private`（指向 `https://github.com/emqx/VeloFlux.git`）。
- `origin` 是个人 fork，用于临时代码迭代；最终 PR 提交到 `private`。
- 工作分支应基于 `private/main`，PR 目标分支为 `private/main`。
- 每次 commit 前必须：`make clippy` 通过、相关测试通过。
