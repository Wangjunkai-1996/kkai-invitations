# 销售邀请：Invitations 第八次迁移维护发布方案

本文是待审查的实施方案，不是已执行记录。目标是交付销售身份、独立人工审批、客户金额看板和新订单七天冷却，同时保留全部既有账本、支付事件和人工发放证据。本次只升级 Invitations 数据库；不迁移 NewAPI schema、不执行 `credit-migrate`、不改变币制或补发历史款项。

## 结论与范围

现有 Invitations 发布机制是单实例 Compose/systemd 替换，不是 NewAPI 的蓝绿控制器。该服务的新程序会在监听 HTTP 之前自动安装第八条 SQLx 迁移，因此不能以“普通蓝绿上线”名义直接执行现有 `make invitations`。

推荐路径为：在 Mac 完成精确产物与隔离演练；取得下述维护范围授权；短暂停止 Invitations，取得新鲜专库备份并验证恢复；启动隔离的新服务完成事务迁移；通过限定的只读验收后恢复事件接收，再开放正常业务。NewAPI 的模型请求、登录和充值服务继续运行，Invitations 页面及其接口在窗口内暂不可用。两者共用 PostgreSQL 资源，仍须设置备份负载和查询超时门禁，不能承诺完全没有资源影响。

2026-10-14 06:10 UTC，主执行者提供的本次只读现场证据为：NewAPI `HEALTHY`，当时 release 为 `/srv/kkai/newapi-releases/kkai-prod-20261013.1791890951-fb2cc6221`、blue、rollback ready；Invitations healthy、settlement enabled、pending/dead/cases 均为 0。本文作者未再次访问生产。该时间点不证明未来窗口健康、当前迁移清单或备份可用；进入窗口前必须重新取证，不硬编码颜色、版本或容器地址。

主执行者本轮另确认前端为 **format 1 / API 1**，current 为 `kkai-frontend-20261013.1791891555-fb2cc6221`；当前 backend manifest 为 `feature`，source `fb2cc6221aa1cc1177dbdef6c8c70eb897bc3d6b`，image `sha256:fbf1561b6be8c2c73a1ab32819b6c47d218660d117313ae0a0c67ab54f861552`。本轮仅按实际 legacy-pair 准备前端产物，不推断 format 2 已安装，也不从 manifest 的 `feature` 字样推断可执行任何数据库迁移。

确认的本地仓库：

| 用途 | 路径 |
| --- | --- |
| Invitations 方案与开发工作树 | `/Users/tokk/Documents/Codex/2026-08-05/li/work/invitations-sales` |
| Invitations 原始发布仓库 | `/Users/tokk/Documents/Codex/2026-08-05/li/work/invitations-current-src` |
| 本次基础设施 checkout | `/Users/tokk/Documents/Codex/2026-08-04/s-y-s-s/kkai-infra` |
| NewAPI 原始发布仓库 | `/Users/tokk/Documents/Codex/2026-08-04/s-y-s-s/new-api-kkai` |

## 已核实的阻断

| 本地代码事实 | 影响与处理 |
| --- | --- |
| infra `roles/invitations/tasks/prerequisites.yml` 强制 `invitations_required_migration_count == 7`；defaults、policy test 和 runbook 也固定为 7。新版 `src/db.rs` 要求八个精确版本，新增 `202610140001`。 | 不能仅覆盖 inventory 为 8，更不能删掉校验。发布契约、校验、测试和文档需一起支持本次明确的 7→8 升级。 |
| `roles/invitations/tasks/deploy.yml` 的 rescue 不检查数据库版本，恢复旧 symlink/unit 后直接重启旧服务。 | 新迁移若已经提交，SQLx 0.8.6 的旧 migrator 不认识 `202610140001`，默认 `ignore_missing=false` 会报 `VersionMissing`。必须依据提交后的真实迁移清单决定恢复路径；禁止无条件启动旧二进制。 |
| `src/main.rs` 正常启动调用 `db::init_pool` / `prepare_database`，在 HTTP、定时任务启动前执行 migration；`--preflight` 仅只读报告，没有 migrate-only CLI。 | 不应编造 `--migrate-only` 命令或绕开 SQLx 手工执行 migration SQL。最小方案是在网络隔离、后台任务关闭的真实新服务启动中完成原生事务迁移。 |
| `SETTLEMENT_MODE=read_only` 只阻止余额结算和销售确认发放；它不是全局写入冻结。 | 仍需隔离用户、管理员、内部 ingest、手动 sync 及所有旧消费者。仅修改 settlement 配置不能作为停写证据。 |
| `AUTO_SYNC_INTERVAL_SECONDS=0` 不停止 topup reconciliation；当前 `run_topup_reconciliation` 会 finalize 账本。某些 GET 也会解锁奖励。 | 隔离验收同时设置 `TOPUP_RECONCILIATION_INTERVAL_SECONDS=0`；只调用明确无副作用的读取接口，不能把所有 GET 当作只读。 |
| infra 备份 runbook 的 NewAPI 核心 DR 只保护 `newapi` 数据库。 | 它不是 `invitations` 的恢复点，不能替代本次专库备份与真实恢复演练。 |

## 需要单独明确的维护授权

已获提交、推送和正常上线授权，不重复索要同一授权。但上述数据库升级、单实例短暂停服和旧版本失去直接回退能力属于新发现的范围变化。生产维护前应一次性确认以下具体范围：

1. 安装经过验证的 Invitations 发布保护改动，包括数据库版本门禁、隔离启动、迁移后禁止旧程序自动回退；相关 infra 提交、推送与安装范围单独列明，不把应用仓库推送授权扩展到 infra。
2. 在约定窗口暂时停止 Invitations 的所有入站和后台消费者；NewAPI 保持服务。停机预算由真实备份/恢复演练耗时、剩余重试预算和退出余量决定，不先承诺固定“几分钟”。
3. 取得并加密保管本次 `invitations` 专库备份，在隔离 PostgreSQL 恢复校验；执行唯一新增迁移 `202610140001`，随后按验收门禁恢复业务。
4. 默认恢复策略为事务提交前恢复旧服务、提交后保留第八次 schema 向前修复或切换经过验证的 schema-8 兼容程序。生产数据库 restore、重置 outbox、真实付款或人造充值不在该授权内，发生时需要另行提出具体恢复/财务操作。

窗口执行者通过既定验收后可连续推进，不再以每个阶段为由重复确认。未取得这一维护范围授权前，只准备本地代码、产物、隔离演练和可审查的执行材料。

## 阶段 A：本地完成产物与发布保护

1. 将经过验证的服务源码集成到原始仓库本地 `production/kkrich`，保持干净，核对完整 source SHA、tree 可读。只提交本任务文件，不丢弃无关修改。代码推送与本地交付分别记录；联网同步失败不能冒充生产失败。
2. 使用 infra `build/invitations/build.sh` 的本地手工链路，`PUSH_IMAGE=false`。应用仓库 `build/kkai-image/export-release.sh` 会 pull GHCR，属于旧产物导出路径，本次不用它替代本机构建。

   ```sh
   SOURCE_DIR=/absolute/confirmed/invitations-production-checkout \
   SOURCE_SHA=REVIEWED_FULL_COMMIT \
   APP_VERSION=UNIQUE_REVIEWED_VERSION \
   PUSH_IMAGE=false ./build/invitations/build.sh
   ./build/invitations/verify-release.sh --verification-mode manual-offline \
     /absolute/artifacts/offline-release.yml REVIEWED_MANIFEST_SHA256
   ```

   以上是已有接口，参数须替换为本次确认值。冻结 manifest SHA256 后才上传；不得从收到的远端副本反算“预期”哈希。保留 Linux amd64、UID 10009、source/image ID/DiffIDs、SBOM、漏洞及 secret scan 证据。镜像构建中的强制测试仍按正式 build/runbook 执行，不用开发阶段的定向测试替代。
3. 在隔离数据库实测：旧七迁移库升级到八；已存在历史账本时计数/额度不变；事务失败回滚仍为七；迁移提交后健康失败时保持维护并禁止重启旧 binary；新程序在八迁移库重复启动幂等。若选择兼容恢复镜像，必须用它通过实际八迁移库验收，不能仅修改 `ignore_missing`。
4. 按 infra `AGENTS.md` 完成行为改动的 `make policy`、Ansible 改动的 `make check` 及正式安装/self-test 门禁。执行前按本次审批规则说明确切命令和预计时长；本文编写阶段不运行这些广域检查。相关 infra 变更必须是干净精确提交，不能把本地源码能力冒充已安装能力。若触及已安装 manual-controller pin，按其正式安装和 app contract 对齐规则处理；不因此迁移 NewAPI schema。
5. 上线前把最终命令、精确产物与镜像 ID、infra SHA、备份位置、隔离验收入口、恢复程序、窗口上限和阶段退出条件汇成一份执行清单。下面描述的维护分支是待实现能力，当前 `make invitations` 不能直接执行它。

## 阶段 B：窗口前证据与 outbox 预算

从已确认的 infra checkout 执行既有只读入口 `make newapi-status`、`make invitations-status`；记录时间与脱敏输出。另取得 Invitations 的七条迁移版本和 checksum、数据库身份/owner、当前 release/source/image identity、账本与待结算聚合。只有 count=7 不足以证明版本集合正确。

进入停服前必须确认没有未完成或状态不明的余额发放/冲正，包括正在请求 NewAPI 的 dispatcher 操作。发现 queued、dispatching、external_pending、retry_wait、needs_review 等待核对状态时，停止推进并按原有对账流程处理，不通过改 status 清零。停止后还要再次确认在途请求结束，防止备份落在外部入账完成、本地凭据未落地的中间状态。

NewAPI outbox 有有限重试，不会永远保持 pending：`service/kkai_outbox.go` 默认最多失败 12 次、从 5 秒退避、上限 1 小时；`service/topup_rebate_outbox_handler.go` 将连接失败、408、429 和 5xx 作为可重试，其余 4xx 会直接标记 dead。

- 停服时保留 NewAPI 原始事件、event key、payload、锁/尝试次数和既有 endpoint/secret。不能删事件、伪造 delivered 或把业务故障改成成功响应。
- 维护响应只允许 503 或连接拒绝这类可重试结果。不能让维护代理返回 401、403、404，也不能暂换 secret 造成鉴权失败。
- 检查该 topic 的 pending 数、最高 attempts、available_at、最长等待、在途 lease 与 dead 增量，据实际剩余预算制定窗口截止时间；预留恢复端点和下一次退避的时间。不能把全新事件的完整 12 次预算套到已有事件。
- 若演练预计耗时加恢复余量不能落在剩余预算内，不进入窗口。先另行设计、测试并授权按 topic 暂停投递的能力，保持支付事务继续产生 outbox。当前服务没有可直接使用的热暂停开关；不能靠清空 endpoint/secret、重启整个 NewAPI 或停止所有 outbox topic 来冒充精准暂停。
- 恢复接收后，已经退避的事件可能不会立即到达；以 attempts/available_at 和收据追踪判断正常等待，不手改 next-attempt 强行刷新。发生新增 dead 时不宣布完成，保留现场并提出单独的对账与重投方案。

## 阶段 C：仅冻结 Invitations，取得专用恢复点

1. 使用受审维护分支阻断全部 Invitations 入口并停止唯一旧服务/旧消费者，确认没有另外的旧实例、手动 sync、cron、维修任务或外部写程序仍可写该数据库。主站及支付回调保持运行。不要照搬历史跨主机首次搬库手册中的“停止全部 NewAPI/Sub2API writer”。
2. 检查只针对 `invitations` 的应用会话、进行中事务与锁；确认停止完成。确需终止残留时，只允许按已确认 database/user 精确选择其会话，不能终止 NewAPI 或 PostgreSQL 全局连接。
3. 在此冻结点记录 `_sqlx_migrations`、返利记录按来源/状态计数与整数额度合计、申请与 item/ownership 对齐、balance_operations/outbox 状态、源事件收据、未解决对账项、同步检查点，以及数据库/程序版本。证据只保留摘要和必要 ID，不输出密码、session、完整客户数据或支付 payload。
4. 对 **`invitations` 单库** 制作本次新鲜 custom-format `pg_dump`，使用匹配 PostgreSQL major 的工具，记录开始/结束 UTC、database identity、工具版本、大小、SHA256 和冻结点摘要。备份写入 root 私有目录并加密留存副本；凭据从既有 secrets 读取，不放在 argv、日志或文档。不得备份/恢复整个共享 PGDATA，也不得操作 NewAPI schema。
5. 对这个确切 SHA256 的 dump 执行 `pg_restore --list` 后，再真实恢复到无生产凭据、无生产网络路由、无公开端口的独立 PostgreSQL；`--no-owner --no-acl` 后在隔离库恢复受审 owner/权限。逐项比对冻结点摘要、约束与迁移 checksum。仅列目录或检查 hash 不算恢复成功。
6. 记录这次完整恢复的真实耗时和结果，再使用隔离副本完成新镜像升级和只读验收。演练所需 NewAPI 数据只能使用已授权的隔离副本/受控 fixture，不能让恢复库的程序向线上 NewAPI 发余额请求。专库备份不是跨库同一时刻快照，也不是 PITR/RPO 承诺。

可以在窗口前先用近期副本演练以缩短不确定性，但最终迁移仍须以停写后取得的本次新鲜 dump 及其成功恢复证据为门禁。若这一步超过窗口预算且迁移尚未提交，恢复原七迁移服务、保持事件可重试，并延期维护；不要为了赶窗口跳过恢复校验。

## 阶段 D：预检、事务迁移与隔离启动

1. 对已经停止写入的生产 Invitations 数据库及 NewAPI 只读连接，运行精确新镜像的已有 `--preflight`。必须保留 JSON、退出码和 release identity；任何 blocking issue 或无法确认的币制一致性都停止。只读 preflight 的 `safe_to_migrate` 不是迁移已执行，也不替代启动时维护锁下的再次预检。
2. 新服务临时配置为 `SETTLEMENT_MODE=read_only`、`AUTO_SYNC_INTERVAL_SECONDS=0`、`TOPUP_RECONCILIATION_INTERVAL_SECONDS=0`，并使用容器内 `LISTEN_ADDR=127.0.0.1:6212`。保持没有 host port。loopback 阻止前端网络和 data 网络的任意调用者直接访问，数据库连接仍正常；通过受审的容器网络命名空间探针做验收。现有 entrypoint healthcheck 支持按 LISTEN_ADDR 检查。
3. 在旧服务确定停止后，由受审维护发布路径启动唯一新实例。不要同时启动新旧消费者，也不要直接执行裸 migration SQL。当前 `prepare_database` 会在事务内取得 advisory/表锁、运行 preflight、执行 SQLx migrator、验证 schema，然后提交；只有完成这些步骤才启动 HTTP/后台任务。
4. 观察锁等待与 `lock_timeout=15s`、`statement_timeout=5min`。超时即失败，不移除超时或扩大权限绕过。根据数据库实际 `_sqlx_migrations` 确认提交结果；网络断开或 healthcheck 失败不等于事务回滚。
5. 提交后核对恰好八条成功迁移、无 failed migration、旧七条 checksum 不变、新表/字段/触发器完整、普通历史申请仍为 ordinary、旧金额和到期时间不变，以及非零且稳定的 `order_rebate_seven_day_cutover_at`。不要把设置的时间改成重启时间。

该边界之前已落账的到期时间保持不变。边界前完成但延迟投递的订单按原二十四小时规则，边界起的新订单按七天规则；初始计算等待仍为五分钟。注册奖励规则不变。旧消费者先停的必要性在于避免边界后仍由旧程序写入二十四小时记录。

## 阶段 E：限定只读验收

保持外部入口关闭、两个 scheduler 为 0、dispatcher 停止。不能仅依据容器 healthy 就开放业务。

- 运行新镜像 `--preflight`，确认与阶段 C 摘要对照无金额、归属、结算证据异常；SQL 聚合使用只读事务。
- 使用经批准的现有会话，验证身份相同的用户只能读取自己的 `sales-profile`、`sales-customers`、受邀客户充值和 `my-rebate-requests`；管理员验证 `sales-accounts`、按 `requestKind` 分类的申请列表和统计。无销售账号时返回未登记状态是正确结果，不为通过验收而创建虚拟销售身份。
- 本阶段不调用会解锁注册奖励的 `rebate-records`、`available-rebates` 或带解锁副作用的管理员读接口；不发送 internal ingest、不手动 sync、不新增申请、不审批、不登记发放。
- 维护模式拒绝 `confirm-issued` 的 handler 回归、并发确认、余额发放隔离、客户权限和币制快照已在本地验证；生产只读阶段不为复现它们而创建订单或财务记录。若需要线上拒绝探针，必须是预先审查且在任何业务写入前必然被维护门禁拒绝的精确请求，并证明前后无业务变化。
- 验证 NewAPI readonly role 的身份/权限没有扩张；不进行 NewAPI DDL、币制转换或写入 canary。保留合成环境与真实现有会话验收证据的区别。

## 阶段 F：按顺序恢复与完成证据

1. 阶段 E 通过后，受审维护分支将新服务监听恢复为正常内部地址，保持相同新镜像、schema 和 `read_only`，先恢复 NewAPI 事件接收。应有 Invitations 范围的入口控制继续阻止用户/管理员写请求；不能靠 settlement read_only 代替该控制。若没有此受审控制，保持 loopback，直至按下一项一次性正式开放；不要暴露假“全局只读服务”。
2. 核对 backlog 正常消化，dead 无新增、receipt/event key 幂等、新旧订单冷却正确且没有重复返利；不要求正常尚未到 available_at 的重试立刻清零。恢复正常用户/管理员入口、原先获准的 reconciliation 周期及 settlement mode 时，使用同一审批范围的精确最终配置。legacy auto-sync 继续为 0，除非另有明确授权。
3. 由执行者完成新的客户读取、普通/销售审批分类和金额状态验证；不自动批准历史申请、确认实际未发的款或伪造支付。已经批准的真实业务操作仍走正常后台流程。
4. Invitations 后端就绪后再交付 NewAPI default 前端。本轮已确认 **format 1 / API 1**，按 runbook 20 的 legacy-pair 路径，将前端精确绑定执行时再次确认的当前 backend release/source/image/schema manifest；只复用已验证的配对坐标，不重建 NewAPI 后端、不 stage 或 promote 后端、不迁移 NewAPI schema。若坐标改变，停止并重新确认配对。本轮不切换到 format 2；未来只有独立安装和验收 format 2 后才能改走 runbook 21。
5. 立即及观察期结束运行正式 `make invitations-status`，观察 pending/dead、lag、未解决对账项、应用错误和有效配置；并以 `make newapi-status` 证实主站仍健康。记录观察时长和是否达到实际重试/对账周期，不用一次 healthcheck 冒充业务验收。
6. 归档 source/infra SHA、产物 manifest hash、迁移清单/时间、备份 hash、恢复结果、阶段配置、outbox 前后摘要、实际停机时间和恢复边界。备份与旧/兼容镜像至少按 Invitations runbook 保留七天；NewAPI 回滚资产按自身规程保留，不做全局 prune。

## 失败恢复判定

| 失败位置 | 允许的恢复 | 禁止的做法 |
| --- | --- | --- |
| 迁移前或事务明确回滚，数据库仍是原七条且 checksum 一致 | 恢复原 release/unit 与原配置，核对旧服务健康及事件恢复。 | 只凭命令退出码假定 schema 没变。 |
| 第八条已经成功提交，但新服务/验收失败 | 保持 Invitations 维护；优先修复并重启精确新版本，或切换已在八迁移库验证的兼容恢复镜像。NewAPI 继续服务，受 outbox 预算保护。 | 旧七迁移 binary 自动 rescue、删除 migration history、设置 ignore_missing 绕过、删新表/销售凭据。 |
| 已恢复消费或发生新的销售/结算写入 | 保留新账本和 outbox 事实，向前修复/对账；必要时提交单独的专库恢复及事件重建计划。 | 用旧 dump 覆盖新财务事实；直接把所有 delivered/dead 改成 pending。 |
| 停机过长或新 dead 事件出现 | 保护事件、停止开放推进，核对 immutable receipt/订单后提出逐条幂等修复。 | 删除事件、按当前配置重算历史金额、声称已经交付完成。 |

单库备份恢复不是普通应用 rollback。恢复 Invitations 到旧点后，NewAPI 可能已有 delivered 事件而恢复库缺少对应收据，必须逐条重建一致性；因此生产 restore 不进入自动 rescue。

## 最小 infra 修改清单

以下是本次交付前必须实现并测试的范围，不是已存在的命令：

1. **明确的迁移契约**：更新 `roles/invitations/defaults/main.yml`、`prerequisites.yml`、`validate.yml`、release manifest 和 reviewed inventory，使本次 artifact 精确对应八条 migration；兼容旧版本时使用明确版本集合，拒绝未知/failed/checksum 不符，不放宽为“至少 7 条”。同步 `roles/invitations/tests/test-policy.sh` 和 runbook 10。
2. **维护启动分支**：在 Invitations role 内加入只针对本服务的阶段控制：验证备份/恢复证据 → 停止旧消费者 → 新镜像 `--preflight` → loopback、read_only、两个 scheduler=0 启动 → 限定验收 → 精确恢复配置。对隔离阶段修改 HTTP 验收位置，不能让现有 frontend-network 探针失败后误入旧 rescue。当前 release 目录以 infra SHA 命名且禁止同名配置变更；临时隔离配置必须有独立受审的维护产物/阶段身份及 hash，不能在同一个 immutable release 目录改 LISTEN_ADDR/settlement 后重用 SHA。最终服务仍使用原本经过验证的完整正式配置。不要改共享 PostgreSQL 或整个 platform stack。
3. **schema-aware rescue**：部署前保存旧 migration identity；失败时重新读取实际清单。七迁移未变才恢复旧 binary；八迁移已提交就停在维护状态并报告 `schema_committed/recovery_required`，保留新 release、证据与备份，不覆盖状态为“已回滚”。若使用兼容恢复 artifact，绑定其 source/image/migration contract 并验证后才启动。
4. **专库备份/演练证据**：添加限于 Invitations 的受审入口和结构化证据，覆盖 dump SHA、隔离恢复结果、冻结点摘要及时间；不复用仅保护 NewAPI 的核心 DR 或共享 PGDATA 快照。不得自动恢复生产数据库。
5. **outbox 门禁与范围隔离**：短窗口使用现有可重试语义并按实际预算停机；长窗口必须先有独立审查的 topic 投递暂停方案。若采用先恢复 ingest 再开放用户写入，需要 Invitations 专用入口 allowlist/503 维护能力，并在隔离环境覆盖旁路和4xx误伤测试。该能力未完成时，不声称支持分阶段公开只读。
6. **故障演练与幂等**：覆盖迁移前拒绝、事务中断、迁移提交后健康失败、断线重试、旧 binary 被拒绝恢复、维护503不产生永久失败、同一八迁移版本二次 apply 无额外改变、兼容恢复的真实可用性。保持强制 source/image/ACL/hardening 检查。

本方案刻意保留单实例短维护路径；没有添加第二个共享写库的 Invitations 实例，也没有把它描述成无停机蓝绿。若未来需要真正的双实例发布，应另行设计 schema 兼容窗口、唯一后台写者、入口切换与 rollback 契约。

## 本文验证与限制

只读核对了上述两个仓库的实际接口、SQLx 迁移启动次序、infra 计数与 rescue、服务环境变量、支付事件重试和既有备份范围。本文只新增方案文档；未运行生产命令、构建、迁移、数据库 restore、提交或推送。文档检查使用 `git diff --check -- docs/sales-invitations-release-plan.md`；未运行全量测试。实际停机耗时、专库备份可恢复性及新维护入口必须由后续演练和执行证据证明。
