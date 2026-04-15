---
spec_id: active-auth-pool
title: Active Auth Pool Routing Design
doc_type: design
owner: Codex
status: draft
version: v1
source:
  - conversation requirement: "支持将路由限制为固定数量的活跃账号，例如 2 个活跃账号参与轮询，其余账号作为候补"
approved: false
approved_by:
approved_at:
---

## 0. Source summary and clarification

### 0.1 Source summary

| Source | Type | What it contributes | Reliability | Notes |
| --- | --- | --- | --- | --- |
| 当前对话 | requirement | 需要把现有多账号路由增强为“固定数量活跃账号 + 候补账号” | High | 用户明确提出“比如 2 个并行度” |
| 当前代码实现 | existing behavior | 现有仅支持 `round-robin` 与 `fill-first`，无活跃池限制 | High | 已定位到 config、selector、scheduler、management 入口 |

### 0.2 Clarification block

| ID | Topic | Source conflict or gap | Current handling |
| --- | --- | --- | --- |
| C1 | “并行度=2” 的真实语义 | 未明确是“总共 2 个账号”还是“单 provider/model 2 个账号” | v1 定义为“每个 provider + model shard 限制 2 个活跃账号/账号组” |
| C2 | Gemini virtual auth 的计数单位 | 现有 gemini virtual auth 是按 parent group 两层轮询，不应按 project 子 auth 计数 | v1 将活跃池单位定义为 “credential group” |
| C3 | Web 面板是否同步支持 | 用户最终确认不需要补 Web UI | 不纳入本次实现范围 |

## 0. Intake check

| Check item | Status | Notes |
| --- | --- | --- |
| Business goal is explicit | Yes | 目标是限制参与轮询的活跃账号数 |
| Non-goals are explicit | Partial | 未明确是否要求首版即支持完整 Web 可视化 |
| Participating systems and callers are known | Yes | CLIProxyAPI 核心路由、管理接口、配置系统 |
| Existing module/process to reuse is identified | Yes | `RoutingConfig`、`RoundRobinSelector`、`FillFirstSelector`、`authScheduler` |
| Persistence impact is known | Yes | 无 DB schema 变更，仅新增配置字段 |
| Compatibility/migration constraints are known | Yes | 默认值必须保持旧行为不变 |
| Permission/audit requirements are known | Partial | 仅需沿用现有 management 鉴权与配置落盘 |
| Observability/rollback expectations are known | Partial | 需要补调试信息与可回滚到旧路由 |

## 1. Background and goals

### 1.1 Background

当前 `CLIProxyAPI` 在同 provider + model 下仅提供两种账号选择语义：

- `round-robin`：所有可用账号参与轮询
- `fill-first`：优先消耗第一个账号，失败或冷却后切到下一个

这对“账号很多，但希望只保持少量账号活跃、其余账号待命”的使用场景不够友好。用户希望例如只让 2 个账号持续承压，其余账号仅在活跃账号不可用时补位。

### 1.2 Goals

- 在现有 `routing` 配置下新增“活跃账号池上限”能力
- 保持现有 `round-robin` / `fill-first` 语义不被破坏
- 对 scheduler fast path 和 legacy selector path 同时生效
- 支持热更新配置
- 为后续管理面板展示“活跃中 / 候补中”状态预留结构

### 1.3 Non-goals

- v1 不修改凭证存储结构，不新增数据库表
- v1 不实现复杂权重、按账号标签限流、按租户独立活跃池
- v1 不保证 mixed-provider 请求的“全局总活跃账号数”限制
- v1 不改原生 Web UI

## 2. Scope and terminology

### 2.1 Scope

本次设计覆盖：

- 配置结构与热更新
- 单 provider 路由
- mixed-provider 路由的兼容行为
- scheduler fast path
- legacy selector path
- management basic config API
- TUI config tab

本次设计暂不覆盖：

- 复杂前端可视化状态面板
- 按 API key / tenant 单独维护活跃池

### 2.2 Terminology

| Term | Meaning | Notes |
| --- | --- | --- |
| candidate | 当前 provider + model 下所有匹配且未被禁用的账号 | 还未经过 cooldown/priority 过滤 |
| ready auth | 当前可立即使用的账号 | 已排除 disabled/cooldown/blocked |
| active pool | 允许直接参与当前常规路由的有限账号集合 | 大小由 `max-active-auths` 决定 |
| standby | 不在 active pool 中，但仍然 ready 的候补账号 | 活跃账号不可用时用于补位 |
| shard | `provider + canonical model` 维度的调度单元 | scheduler 当前已存在该概念 |
| credential group | Gemini virtual auth 的 parent 级分组 | 活跃池对该场景按 group 计数 |

## 3. Architecture

### 3.1 Use cases

- Codex 多账号池只让 2 个账号轮询，其余账号待机
- Gemini virtual auth 场景只让 2 个 parent credential 参与外层轮询
- `fill-first` 模式保持原行为不变
- 活跃账号进入 cooldown / quota exhausted / disabled 后自动从候补补位

### 3.2 Functional architecture

核心修改模块：

- `internal/config/config.go`
  - 新增路由配置字段
- `sdk/cliproxy/auth/selector.go`
  - legacy selector 路径下新增 active pool 选择逻辑
- `sdk/cliproxy/auth/scheduler.go`
  - fast path 下新增 active pool 视图/状态
- `sdk/cliproxy/auth/conductor.go`
  - 将 runtime config 下发给 selector 与 scheduler
- `internal/api/handlers/management/config_basic.go`
  - 暴露配置读写接口
- `internal/tui/config_tab.go`
  - 提供最小可配置入口

### 3.3 System architecture

本方案不改变现有协议转发、OAuth、token store、watcher、registry 架构，只在“auth 选择层”插入一层 active pool 限制：

1. 配置加载
2. runtime config 进入 `Manager`
3. `selector` 与 `scheduler` 读取 `max-active-auths`
4. 单 provider 或 mixed-provider 请求选择时，在 ready candidates 上应用 active pool 规则

## 4. Process design

### 4.1 配置加载与热更新

#### Trigger

- 服务启动
- watcher 检测到 `config.yaml` 变更
- management API 更新 routing 配置

#### Main flow

1. `RoutingConfig` 新增 `max-active-auths`
2. `Service` reload 时继续通过 `SetSelector()` 切换策略
3. `Manager.SetConfig()` 存储最新 runtime config
4. 若 selector 实现了可选的 config-aware 接口，则同步下发配置
5. scheduler 同步更新 active pool 限制

#### Branch rules

- `routing.strategy != round-robin` -> 忽略 `max-active-auths`
- `max-active-auths <= 0` -> 关闭活跃池限制，保持旧行为
- `max-active-auths == 1` -> 退化为“单活跃账号”
- `max-active-auths >= ready candidate count` -> 等价于不限制

### 4.2 单 provider 路由流程

#### Preconditions

- 已完成 provider 过滤、model 支持检查、disabled/cooldown 过滤、priority 选择

#### Main flow

1. 取得最高优先级的 ready candidates
2. 若当前策略不是 `round-robin`，沿用旧逻辑
3. 若当前策略是 `round-robin` 且开启 active pool：
   1. 基于 shard 读取当前 active membership
   2. 保留仍然有效的 active members
   3. 若活跃成员不足上限，从 standby 中按稳定顺序补位
   4. 在 active members 内继续执行 `round-robin` 或 `fill-first`
4. 返回选中的 auth

#### Branch rules

- 活跃池中的账号进入 cooldown/disabled -> 从 active membership 移除，下次 pick 补位
- 当前请求的 `tried` 已排除部分活跃账号 -> 允许本次 pick 临时使用 standby，但不永久改写全局 active membership
- pinned auth 存在 -> pinned auth 仍然优先，活跃池限制不覆盖显式 pin

#### Failure and fallback

- active pool 中所有账号都被当前请求 `tried` 排除 -> 回退到 standby eligible candidates
- 所有 ready candidates 都无可用账号 -> 保持现有 `auth_unavailable` / `model_cooldown` 语义

### 4.3 Gemini virtual auth 分组流程

#### Main rule

当 `readyView` 为 grouped view（即 `gemini_virtual_parent` 两层轮询）时，活跃池限制作用于 parent group，而不是 project 子 auth。

#### Example

- 有 5 个 parent credential，每个 parent 下 3 个 project auth
- `max-active-auths=2`
- 系统只会选择 2 个 parent group 作为 active groups
- group 内部仍沿用现有 child round-robin

### 4.4 Mixed-provider 路由流程

#### Main rule

v1 对 mixed-provider 请求不做“全局活跃数限制”，而是：

- 先在每个 provider shard 内独立应用 active pool
- 再沿用现有 mixed-provider 选择逻辑

#### Rationale

- 改动小，兼容现有 scheduler 结构
- 不会把多 provider 混合请求的状态管理复杂化
- 更符合“某 provider 的账号池控制”语义

## 5. Business rules and state transitions

### 5.1 Core rules

1. `routing.max-active-auths` 仅限制 ready auth 的常规参与范围，不改变 disabled/cooldown/priority 规则。
2. active pool 以 `provider + canonical model + priority bucket` 为作用域。
3. 对 flat auth 集合，active unit 是 auth；对 grouped Gemini virtual auth，active unit 是 parent group。
4. request-local `tried` 只影响当前 pick 的临时 fallback，不应永久污染全局 active membership。
5. `fill-first` 模式完全忽略 `max-active-auths`，保持原 deterministic 选择顺序。
6. `round-robin` 模式下，active pool 内继续 round-robin，standby 只在补位或 request-local fallback 时被使用。

### 5.2 State machine

| Object | Current state | Trigger | Next state | Reversible |
| --- | --- | --- | --- | --- |
| auth member | standby | active pool 空位补位 | active | Yes |
| auth member | active | cooldown / disabled / blocked | standby or removed | Yes |
| auth member | active | current request marked tried | temporary skipped | Yes |
| auth member | standby | no active candidate matches request-local predicate | temporary fallback candidate | Yes |

## 6. Data design

### 6.1 Config changes

在 `RoutingConfig` 中新增：

```yaml
routing:
  strategy: "round-robin"
  max-active-auths: 2
```

建议字段定义：

```go
type RoutingConfig struct {
    Strategy       string `yaml:"strategy,omitempty" json:"strategy,omitempty"`
    MaxActiveAuths int    `yaml:"max-active-auths,omitempty" json:"max-active-auths,omitempty"`
}
```

字段语义：

- `0` 或缺省：禁用该能力，保持全量路由
- `1+`：限制最大活跃 auth / group 数

### 6.2 Persistence impact

- 无数据库迁移
- 无 token store schema 变更
- 配置仍落在现有 `config.yaml`

## 7. Interface and config design

### 7.1 Config API

在现有 basic config handler 基础上新增：

- `GET /v0/config/routing/max-active-auths`
- `PUT /v0/config/routing/max-active-auths`

校验规则：

- body 结构沿用现有 `{ "value": <int> }`
- 仅允许 `>= 0`
- 非法值返回 `400`

### 7.2 TUI

在 `internal/tui/config_tab.go` 中新增：

- `Routing Max Active Auths`

## 8. Implementation design

### 8.1 Backward-compatible config propagation

为避免破坏现有自定义 selector 接口，采用可选接口：

```go
type configAwareSelector interface {
    SetConfig(*internalconfig.Config)
}
```

`RoundRobinSelector` 实现该接口并缓存 `max-active-auths`。`FillFirstSelector` 不消费该配置。

`Manager.SetConfig()` 在 `runtimeConfig.Store(cfg)` 后执行：

1. 如果 selector 支持 `SetConfig`，则同步下发
2. 调用 `scheduler.setConfig(cfg)`

### 8.2 Selector path

`selector.go` 中新增：

- 解析 `max-active-auths`
- flat auth 的 active pool 维护
- grouped Gemini virtual auth 的 active parent 维护
- request-local fallback 逻辑

建议新增内部结构：

- `activeWindowState`
- `activeMembership`
- `standbyCursor`

### 8.3 Scheduler path

由于大多数请求会走 `authScheduler` fast path，必须同步支持 active pool。

建议在 `readyView` 层增加“活跃窗口状态”，而不是只改 `modelScheduler`：

- flat view：按 auth 维度维护 active membership
- grouped view：按 parent group 维度维护 active membership

这样可以最大程度复用现有：

- `pickFirst`
- `pickRoundRobin`
- `pickGroupedRoundRobin`

仅在进入这些选择前先裁剪出 active candidates。

### 8.4 Mixed-provider compatibility

mixed-provider 路由保留当前的两段逻辑：

1. 先计算各 provider shard 的 ready bucket
2. 再按 provider 层 round-robin/fill-first 取 provider

active pool 只影响第 1 步的 provider 内部 ready count，不新增跨 provider 的统一状态机。

## 9. Touch points

预计修改文件：

- `internal/config/config.go`
- `config.example.yaml`
- `sdk/cliproxy/auth/selector.go`
- `sdk/cliproxy/auth/scheduler.go`
- `sdk/cliproxy/auth/conductor.go`
- `sdk/cliproxy/builder.go`
- `sdk/cliproxy/service.go`
- `internal/api/handlers/management/config_basic.go`
- `internal/tui/config_tab.go`
- `README.md`
- `sdk/cliproxy/auth/selector_test.go`
- `sdk/cliproxy/auth/scheduler_test.go`
- `sdk/cliproxy/auth/conductor_scheduler_refresh_test.go`

## 10. Rollout plan

### Phase 1

- 后端配置字段
- selector + scheduler 生效
- management API 支持
- TUI 支持
- 单元测试补齐

### Phase 2

- 可选增加 active / standby 状态展示
- 可选增加调试接口或日志增强

## 11. Risks and mitigations

### Risk 1: 只改 selector，不改 scheduler，线上行为不一致

- Mitigation: v1 明确要求两条路径同时支持，并补对应测试

### Risk 2: request-local retry 污染全局 active membership

- Mitigation: standby fallback 仅对当前 pick 生效，不写回全局活跃池状态

### Risk 3: Gemini virtual auth 计数错误

- Mitigation: grouped view 以 parent group 为 active unit，保留现有 child round-robin

### Risk 4: 热更新后 selector 与 scheduler 配置不一致

- Mitigation: `Manager.SetConfig()` 作为唯一配置下发入口

### Risk 5: mixed-provider 用户误以为是“全局只活跃 2 个账号”

- Mitigation: 文档和 UI 文案中明确作用域是 provider + model shard

## 12. Open questions

1. v1 是否需要把 `max-active-auths` 同时支持到 Web 面板保存页，还是接受先手改 YAML。
2. 是否需要在 auth list 中增加一个只读调试字段，例如 `routing_state=active|standby`。
3. 后续是否要扩展为更强的配置，例如：
   - `routing.active-pool-scope: provider-model | provider`
   - `routing.active-pool-sticky: true | false`
   - `routing.active-pool-policy: sticky-window | rolling-window`

## 13. Recommendation

推荐按以下策略落地：

1. v1 先做 `routing.max-active-auths`
2. 作用域固定为 `provider + model shard`
3. grouped Gemini virtual auth 按 parent group 计数
4. mixed-provider 不做全局限制
5. 先完成后端、测试、management API、TUI
6. 原生 Web UI 不做

这样改动面最小，和当前架构最贴合，也最适合后续继续同步 upstream。
