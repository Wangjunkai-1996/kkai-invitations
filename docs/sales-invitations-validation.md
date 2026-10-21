# 销售邀请开发验证记录

2026-10-14。本次只开发和本地验证，未提交、合并、推送或部署。

## 工作树

| 仓库 | 新工作树 | 分支与起点 |
| --- | --- | --- |
| NewAPI 前端 | `/Users/tokk/.codex/worktrees/sales-invitations/new-api-kkai` | `codex/sales-invitations`，`fb2cc6221aa1cc1177dbdef6c8c70eb897bc3d6b` |
| Invitations 服务 | `/Users/tokk/Documents/Codex/2026-08-05/li/work/invitations-sales` | `codex/sales-invitations`，`0d128800bf704bbb84d9e8a84049ffab030b2198` |

两个原始 `production/kkrich` 工作目录均保持干净。账本安全、冷却时程、客户统计、前端产品分别并行实现并交叉核对。

## 后端：66 项通过

在 Invitations 新工作树执行。Cargo 使用既有缓存；缺少的锁定依赖已下载，`Cargo.lock` 未修改。所有数据库验证指向本次专用临时 PostgreSQL 容器，不使用生产连接。

```sh
export CARGO_TARGET_DIR=/Users/tokk/Documents/Codex/2026-08-05/li/work/invitations-current-src/target
export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:63870/postgres
```

| 实际命令 | 结果 |
| --- | --- |
| `cargo test --offline --locked rebate_lifecycle::tests -- --nocapture` | 4 项通过 |
| `cargo test --offline --locked rebate_source_events::tests -- --nocapture` | 11 项通过 |
| `cargo test --offline --locked sales_customers::tests -- --include-ignored --nocapture` | 2 项通过；最终运行包含自有注册奖励排除回归 |
| `cargo test --locked sales_requests_require_manual_approval_and_isolate_external_confirmation -- --ignored --nocapture` | 1 项真实数据库综合回归通过 |
| `cargo test --locked credit_projection_transaction_preserves_ledger_and_restores_exactly -- --ignored --nocapture` | 1 项真实数据库币制回归通过 |
| `cargo test --locked readiness_manifest_covers_every_migration_catalog_object -- --ignored --nocapture` | 1 项数据库约束、索引、触发器清单回归通过 |
| `cargo test --locked empty_database_migrates_and_reruns_idempotently -- --ignored --nocapture` | 1 项空库升级与幂等回归通过 |
| `cargo test --locked latest_legacy_fixture_is_adopted_without_data_loss -- --ignored --nocapture` | 1 项旧库无损升级回归通过 |
| `cargo test --locked routes::user::tests -- --nocapture` | 8 项通过 |
| `cargo test --locked payout::tests -- --nocapture` | 10 项通过 |
| `cargo test --locked routes::admin::tests -- --nocapture` | 26 项通过 |

销售综合回归实际调用申请、审批、单笔/批量/直接付款、人工发放确认、查看记录等服务处理函数，使用真实 PostgreSQL 事务和新迁移。覆盖强制人工审批、重复及并发确认、归类冻结、停用后只读、维护门禁、数据库直接绕过发放被拒绝、状态筛选/计数/金额对齐。

```sh
rustfmt --edition 2021 --check src/sales_customers.rs src/sales.rs src/credit_currency.rs src/db.rs src/main.rs src/payout.rs src/preflight.rs src/rebate_lifecycle.rs src/rebate_source_events.rs src/routes/admin.rs src/routes/system.rs src/routes/user.rs
git diff --check
```

以上格式与 diff 检查通过。

## 前端：6 文件、18 个不同测试通过

以下实际命令在 NewAPI 新工作树 `web/default` 执行。修改后的文件按受影响范围复测，数量按不同测试去重，不累计重复执行次数。

```sh
bunx --no-install vitest run src/features/invitations/status.test.ts src/features/invitations/admin-action-policy.test.ts src/features/invitations/__tests__/status-label.test.ts src/features/invitations/__tests__/sales-approvals.test.tsx
bunx --no-install vitest run src/features/invitations/__tests__/sales-customers.test.tsx
bunx --no-install vitest run src/features/invitations/__tests__/sales-approvals.test.tsx src/features/invitations/__tests__/status-label.test.ts src/features/invitations/__tests__/sales-customers.test.tsx
bunx --no-install vitest run src/features/invitations/__tests__/sales-approvals.test.tsx src/features/invitations/status.test.ts
bunx --no-install vitest run src/features/invitations/__tests__/sales-records.test.tsx
bunx --no-install oxlint -c .oxlintrc.json src/features/invitations src/routes/_authenticated/invitations src/hooks/use-sidebar-data.ts src/i18n/static-keys.ts
bunx --no-install tsgo -p /tmp/newapi-sales-typecheck.json
bun run i18n:sync
git diff --check
```

均通过。临时 TypeScript 配置扩展该工作树的 `tsconfig.app.json`，限定 invitations 功能、其路由和 sidebar 及必要的全局声明/路由类型依赖，关闭增量写入；不执行整个仓库的 typecheck/build 命令。仓库内临时配置和翻译脚本已清理。

邀请功能使用的 218 个翻译键在 `en/zh/zh-TW/fr/ja/ru/vi` 均存在，插值参数匹配。销售客户表、金额、历史未知金额、审批按钮、资金维护及普通用户行为均用实际 React 组件和模拟 HTTP 验证。

## 边界与待上线验收

- 开发验证阶段未运行完整测试套件或生产构建，未执行生产迁移或状态查询；后续发布授权与维护门禁见 [维护发布方案](sales-invitations-release-plan.md)。
- 未执行浏览器视觉验收、真实 NewAPI 会话与 Invitations 的联合登录验收、真实支付渠道验收。现有认证未改动；新增客户授权与销售审批在处理函数和数据库层已有定向验证。
- 旧订单缺少实际到账快照时明确显示待核对，不把未知金额当作已确认收入。跨服务事件同步仍有延迟。
- 新订单按迁移边界恢复七天冷却；历史已落账到期时间和注册奖励的原有生效规则不重写。
- 生产交付须按 [实施说明](sales-invitations.md) 和既有发布手册另行执行，不能只把后端切回不识别新销售 schema 的旧版本。

## 2026-10-22 销售账号业绩扩展

独立工作树 `/Users/tokk/.codex/worktrees/sales-account-performance/kkai-invitations`，分支 `codex/sales-account-performance`，起点 `4ce8749f02b6d25afb694058b098b46271a8bcce`。本次只扩展只读查询和响应，不新增迁移、依赖或财务写入。

使用本次新建的隔离 PostgreSQL 容器 `kkai-sales-performance-test-20261022`（既有本地 `postgres:18.4-bookworm` 镜像，`--pull never`），两个独立连接各使用 TEMP 表；未连接生产数据库。

```sh
CARGO_TARGET_DIR=/Users/tokk/Documents/Codex/2026-08-05/li/work/invitations-current-src/target \
TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:56203/postgres \
cargo test --offline --locked sales_customers::tests -- --include-ignored --nocapture

rustfmt --edition 2021 --check src/sales_customers.rs src/sales.rs
git diff --check
```

定向测试 **3 项全部通过**（包含 2 项真实数据库测试），原有客户/充值统计回归通过。新增覆盖 41 名销售的 `20/20/1/0` 分页、全局汇总不随页面变化、停用销售保留统计、零业绩结构、已不存在的销售用户名回退、全局与各销售金额舍入一致、客户页与管理员业绩一致、全部返利分类、空账号列表及页码溢出。定向测试首次使用缓存编译约 11 秒，数据库用例约 0.2 秒。

未运行全量测试、生产构建、迁移或部署；本记录不证明生产版本、认证联调或生产查询性能。现有管理员提取器和权限规则未修改；新接口复用其访问保护。
