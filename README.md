# Invitations — new-api 邀请返利系统

外嵌式邀请返利管理系统，为 new-api 提供完整的邀请码、返利记录、提现管理功能。

**核心特性：零修改 new-api 代码**，通过同域反向代理 + Rust 直接解析 session cookie 实现。

## 架构

```
┌──────────────┐     ┌──────────────────────────────────┐     ┌──────────────┐
│  new-api     │────▶│  dli.li/invitations/api/*         │────▶│  Invitations │
│  前端        │     │  (直接读取同域 session cookie)     │     │  后端服务    │
└──────────────┘     └──────────────────────────────────┘     └──────────────┘
```

## 功能特性

### 用户功能
- **邀请管理**：邀请码展示、二维码生成、邀请链接复制
- **返利记录**：返利记录列表（分页、筛选）、返利趋势图（30天）
- **提现管理**：提现申请、提现记录查询

### 管理员功能
- **返利规则**：配置不同用户组的返利比例（订阅/充值）
- **提现审批**：审批用户提现申请（通过/拒绝/完成）
- **统计报表**：系统返利统计、返利趋势图、用户返利详情

### 系统功能
- **自动同步**：服务内置后台任务，默认每小时从 new-api 数据库同步订单并自动计算返利
- **ULID 标识**：每条返利记录使用 ULID 唯一标识，支持幂等性

## 工作原理

1. **同域部署**：Invitations API 通过 nginx 反向代理挂载在主站 `/invitations/api/` 路径下
2. **Session 解析**：直接解析 gorilla/securecookie 格式（HMAC-SHA256 验证 + gob 反序列化）
3. **数据同步**：内置后台任务从 new-api 数据库读取订单，计算返利并写入 invitations 数据库
4. **前端集成**：new-api 前端通过 `/invitations/api/*` 调用 API，页面路由为 `/invitations`

## 前置条件

- Rust 1.85+（项目使用 edition 2021）
- PostgreSQL 数据库
- new-api 部署在同一域名下
- new-api 使用 PostgreSQL 数据库（用于只读查询订单数据）

## 快速开始

### 1. 配置环境变量

```bash
cp .env.example .env
```

编辑 `.env`：

```env
# 必须与 new-api 的 SESSION_SECRET 环境变量相同
SESSION_SECRET=your-new-api-session-secret

# 系统同步接口密钥（手动触发同步接口时需要携带）
SYNC_SECRET=change-this-random-sync-secret

# Invitations 数据库（读写）
DATABASE_URL=postgresql://user:password@localhost/invitations

# new-api 数据库（只读，用于同步订单）
NEWAPI_DATABASE_URL=postgresql://user:password@localhost/newapi

# 监听地址
LISTEN_ADDR=0.0.0.0:6212

# 路径前缀（留空，由 nginx 处理路径重写）
BASE_PATH=

# 内置自动同步间隔，默认 3600 秒（每小时一次）；设置为 0 可关闭
AUTO_SYNC_INTERVAL_SECONDS=3600

# 服务启动后首次自动同步延迟，默认 10 秒
AUTO_SYNC_INITIAL_DELAY_SECONDS=10
```

### 2. 初始化数据库

```bash
# 创建数据库
createdb invitations

# 运行迁移（首次启动时自动执行）
cargo run --release
```

数据库表会自动创建：
- `rebate_rules` - 返利规则
- `rebate_records` - 返利记录
- `withdrawal_requests` - 提现申请
- `system_config` - 系统配置

### 3. 编译运行

```bash
cargo build --release
./target/release/invitations
```

### 4. 配置 nginx

在主站 nginx 配置中添加 location 块：

```nginx
# dli.li 的 server 块中添加：

# Invitations API（必须在 /invitations 之前）
location ^~ /invitations/api/ {
    proxy_pass http://127.0.0.1:6212/api/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header Cookie $http_cookie;
}

# 前端页面路由（由 new-api 处理）
location /invitations {
    proxy_pass http://127.0.0.1:3000;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

完整配置参考 `nginx/invitations.conf`。

### 5. 配置自动同步

服务默认启用内置自动同步：启动后 10 秒先同步一次，之后每 3600 秒同步一次。一般不再需要配置 cron。

如需调整间隔：

```env
AUTO_SYNC_INTERVAL_SECONDS=1800
AUTO_SYNC_INITIAL_DELAY_SECONDS=10
```

如需完全关闭内置自动同步，改用外部 cron 或 systemd timer：

```env
AUTO_SYNC_INTERVAL_SECONDS=0
```

外部 cron 示例：

```bash
# 每小时同步一次
0 * * * * curl -X POST http://localhost:6212/api/system/sync-rebates -H "X-Sync-Secret: change-this-random-sync-secret"
```

外部 systemd timer 示例：

```ini
# /etc/systemd/system/invitations-sync.timer
[Unit]
Description=Invitations Rebate Sync Timer

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

```ini
# /etc/systemd/system/invitations-sync.service
[Unit]
Description=Invitations Rebate Sync

[Service]
Type=oneshot
ExecStart=/usr/bin/curl -X POST http://localhost:6212/api/system/sync-rebates -H "X-Sync-Secret: change-this-random-sync-secret"
```

启用定时器：

```bash
systemctl enable --now invitations-sync.timer
```

### 6. 配置返利规则

通过管理员界面（`/invitations/admin`）或 API 配置返利规则：

```bash
# 创建订阅返利规则（default 用户组，5% 返利）
curl -X POST http://localhost:6212/api/admin/rebate-rules \
  -H "Content-Type: application/json" \
  -H "Cookie: session=your-admin-session" \
  -d '{
    "user_group": "default",
    "rule_type": "subscription",
    "rebate_rate": "0.05"
  }'

# 创建充值返利规则（vip 用户组，10% 返利）
curl -X POST http://localhost:6212/api/admin/rebate-rules \
  -H "Content-Type: application/json" \
  -H "Cookie: session=your-admin-session" \
  -d '{
    "user_group": "vip",
    "rule_type": "topup",
    "rebate_rate": "0.10"
  }'
```

## API 端点

### 用户 API（`/invitations/api/invitations/*`）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/my-code` | 获取我的邀请码和统计信息 |
| GET | `/rebate-records` | 获取返利记录列表（支持分页和筛选） |
| GET | `/available-rebates` | 获取可提现金额 |
| POST | `/request-withdrawal` | 申请提现 |
| GET | `/my-requests` | 获取我的提现申请列表 |

### 管理员 API（`/invitations/api/admin/*`）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/rebate-rules` | 获取返利规则列表 |
| POST | `/rebate-rules` | 创建返利规则 |
| GET | `/user-groups` | 获取用户组列表 |
| GET | `/system-config` | 获取系统配置 |
| PUT/POST | `/system-config` | 更新系统配置 |
| GET | `/withdrawal-requests` | 获取提现申请列表 |
| POST | `/withdrawal-requests/:id/approve` | 批准提现申请 |
| POST | `/withdrawal-requests/:id/reject` | 拒绝提现申请 |
| POST | `/withdrawal-requests/:id/complete` | 标记提现完成 |
| GET | `/rebate-stats` | 获取返利统计 |
| GET | `/user-rebates/:user_id` | 获取用户返利详情 |
| GET | `/users/:user_id/rebate-details` | 获取用户返利详情（前端兼容路径） |
| PUT | `/rebate-rules/:id` | 更新返利规则 |
| DELETE | `/rebate-rules/:id` | 删除返利规则 |

### 系统 API（`/invitations/api/system/*`）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/sync-rebates` | 手动触发返利同步，需要 `X-Sync-Secret`、`Authorization: Bearer <SYNC_SECRET>` 或 `?secret=` |

## 数据同步逻辑

### 订阅返利
1. 从 new-api 的 `subscription_orders` 表读取新订单（`status = 'success'`）
2. 查找订单用户的 `inviter_id`（邀请人）
3. 根据邀请人的用户组查找返利规则（`rule_type = 'subscription'`）
4. 计算返利金额：`订单金额 × 返利比例 × 500000`（new-api 额度单位）
5. 创建返利记录（`status = 'pending'`，使用 ULID 防止重复）

### 充值返利
1. 从 new-api 的 `top_ups` 表读取新订单（`status = 'success'`），并排除已在 `subscription_orders` 中存在的订阅支付镜像记录
2. 查找订单用户的 `inviter_id`（邀请人）
3. 根据邀请人的用户组查找返利规则（`rule_type = 'topup'`）
4. 计算返利金额：`充值金额 × 返利比例 × 500000`（new-api 额度单位）
5. 创建返利记录（`status = 'pending'`，使用 ULID 防止重复）

### 幂等性保证
- 每条返利记录使用 ULID 作为唯一标识
- ULID 格式为标准 26 字符 ULID，例如 `01HQZX3Y4K5M6N7P8Q9R0S1T2U`
- 同步时通过 `(order_type, order_id)` 唯一约束避免重复创建
- 订阅支付会由 new-api 镜像到 `top_ups`，同步充值订单时会排除这些镜像记录，避免同一订阅按充值和订阅重复返利
- 如果缺少返利规则，同步不会推进到该订单之后，避免后续补规则时永久跳过订单

## 提现流程

1. **用户申请**：用户在前端选择可提现的返利记录，输入提现金额
2. **管理员审批**：管理员在管理界面审批提现申请（通过/拒绝）
3. **手动放款**：管理员在 new-api 后台手动增加用户余额
4. **标记完成**：管理员在管理界面标记提现已完成
5. **状态更新**：返利记录状态从 `approved` 变为 `completed`

## 安全说明

- Session cookie 通过 HMAC-SHA256 验证完整性，未通过验证的 cookie 会被拒绝
- 管理员 API 需要 `role >= 10` 权限
- 系统同步 API 需要 `SYNC_SECRET`
- new-api 数据库连接为只读，不会修改 new-api 数据
- 数据库存储的返利金额使用 new-api 额度单位；HTTP API 返回给前端的金额使用人民币分
- 返利记录使用 ULID 唯一标识，保证幂等性

## 技术细节

### Session 解码

invitations 直接解析 new-api 的 gorilla/securecookie v1.1.1 格式：

1. Base64 URL 解码 cookie 值
2. 分割为 `timestamp|value|mac` 三部分
3. HMAC-SHA256 验证（密钥 = SESSION_SECRET）
4. 检查时间戳未过期（30 天）
5. Base64 URL 解码 value 部分
6. Go gob 格式反序列化，提取 `id`、`username`、`role`、`status`、`group`

### 数据库设计

- **rebate_rules**：返利规则表，支持按用户组和规则类型配置不同的返利比例
- **rebate_records**：返利记录表，记录每笔返利的详细信息（订单类型、金额、状态等）
- **withdrawal_requests**：提现申请表，记录用户的提现申请和审批状态
- **system_config**：系统配置表，存储最低提现金额、提现频率等配置

### 前端集成

前端代码位于 `new-api/web/default/src/features/invitations/`：

- **用户界面**：`/invitations`（3 个 Tab：我的邀请、返利记录、提现管理）
- **管理员界面**：`/invitations/admin`（3 个 Tab：返利规则、提现审批、统计报表）
- **技术栈**：React 19 + Rsbuild + Base UI + Tailwind CSS + recharts
- **API 调用**：通过 `/invitations/api/*` 调用后端 API

## 开发

```bash
# 运行测试
cargo test

# 开发模式运行
RUST_LOG=debug cargo run

# 格式化代码
cargo fmt

# 代码检查
cargo clippy
```

## 故障排查

### 返利未同步
1. 检查服务日志中是否有 `Automatic rebate sync scheduler started`
2. 确认 `AUTO_SYNC_INTERVAL_SECONDS` 未设置为 `0`
3. 检查 new-api 数据库连接是否正常
4. 检查返利规则是否配置正确
5. 查看日志：`RUST_LOG=debug ./target/release/invitations`

### Session 解析失败
1. 确认 `SESSION_SECRET` 与 new-api 一致
2. 检查 cookie 是否正确传递（nginx 配置）
3. 检查用户是否已登录 new-api

### 提现申请失败
1. 检查可提现金额是否足够
2. 检查是否满足提现频率限制
3. 检查返利记录状态是否为 `pending`

## License

AGPL-3.0
