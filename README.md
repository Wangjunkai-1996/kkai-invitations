# Invitations - 邀请返利系统

独立的邀请返利管理系统，使用 Rust + Axum 实现。

## 功能特性

- **用户功能**：
  - 查看邀请码和邀请统计
  - 查看返利记录（不显示被邀请人信息）
  - 申请提现（受最低金额和频率限制）
  - 查看提现申请状态

- **管理员功能**：
  - 按用户组配置返利规则（充值/订阅）
  - 配置提现限制（最低金额、频率）
  - 审批提现申请
  - 标记放款完成
  - 查看平台返利统计

- **系统功能**：
  - 定时同步返利记录（每 5-10 分钟）
  - 防重复申请机制
  - 完整的审批流程

## 架构设计

- **独立服务**：使用独立的 PostgreSQL 数据库
- **只读集成**：只读 new-api 数据库，不修改任何数据
- **Nginx 集成**：通过反向代理集成到现有系统
- **Session 验证**：读取 new-api 的 session cookie 进行身份验证

## 快速开始

### 1. 环境准备

```bash
# 复制环境变量配置
cp .env.example .env

# 编辑 .env 文件，配置数据库连接和 session 密钥
vim .env
```

### 2. 数据库初始化

创建独立的 PostgreSQL 数据库：

```sql
CREATE DATABASE invitations;
```

程序启动时会自动创建表结构。

### 3. 编译运行

```bash
# 开发模式
cargo run

# 生产模式
cargo build --release
./target/release/invitations
```

### 4. Nginx 配置

```nginx
location /invitations/ {
    proxy_pass http://127.0.0.1:6212;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header Cookie $http_cookie;
}
```

### 5. 配置定时任务

添加 cron 任务，每 5 分钟同步一次返利记录：

```bash
*/5 * * * * curl -X POST http://127.0.0.1:6212/invitations/api/system/sync-rebates
```

## API 文档

### 用户 API

- `GET /api/invitations/my-code` - 获取我的邀请码和统计
- `GET /api/invitations/rebate-records` - 获取我的返利记录列表
- `GET /api/invitations/available-rebates` - 获取可申请返利的订单列表
- `POST /api/invitations/request-withdrawal` - 提交提现申请
- `GET /api/invitations/my-requests` - 获取我的提现申请记录

### 管理员 API

- `GET /api/admin/rebate-rules` - 获取所有返利规则
- `POST /api/admin/rebate-rules` - 创建/更新返利规则
- `GET /api/admin/user-groups` - 获取所有用户组列表
- `GET /api/admin/system-config` - 获取系统配置
- `POST /api/admin/system-config` - 更新系统配置
- `GET /api/admin/withdrawal-requests` - 获取提现申请列表
- `POST /api/admin/withdrawal-requests/:id/approve` - 通过提现申请
- `POST /api/admin/withdrawal-requests/:id/reject` - 拒绝提现申请
- `POST /api/admin/withdrawal-requests/:id/complete` - 标记放款完成
- `GET /api/admin/rebate-stats` - 获取平台返利统计
- `GET /api/admin/user-rebates/:user_id` - 获取用户返利明细

### 系统 API

- `POST /api/system/sync-rebates` - 同步返利记录

## 数据库表结构

### rebate_rules（返利规则表）
- 按用户组配置充值/订阅返利比例
- 支持不同用户组设置不同比例

### rebate_records（返利记录表）
- 记录每笔返利的详细信息
- 通过 (order_type, order_id) 唯一索引防止重复

### withdrawal_requests（提现申请表）
- 记录用户的提现申请
- 状态流转：pending → approved → completed

### system_config（系统配置表）
- 最低提现金额
- 提现频率限制

### sync_log（同步日志表）
- 记录上次同步的最大 ID
- 避免重复扫描

## 提现流程

1. **用户申请**：选择返利记录，提交申请（检查限制）
2. **待审核**：管理员查看申请列表
3. **审批通过**：状态变为"待放款"
4. **手动修改余额**：管理员在 new-api 后台手动增加用户余额
5. **标记完成**：管理员点击"放款完成"，状态变为"已发放"

## 配置说明

### 环境变量

- `LISTEN_ADDR`：监听地址（默认：127.0.0.1:6212）
- `BASE_PATH`：路径前缀（默认：/invitations）
- `DATABASE_URL`：invitations 数据库连接（读写）
- `NEWAPI_DATABASE_URL`：new-api 数据库连接（只读）
- `SESSION_SECRET`：new-api 的 session 密钥

### 系统配置（可通过 API 修改）

- `min_withdrawal_amount`：最低提现金额（默认：1000000，即 new-api 的额度单位）
- `withdrawal_frequency_days`：提现频率限制（默认：7 天）

## 注意事项

1. **完全只读**：系统完全不修改 new-api 数据库的任何数据
2. **手动放款**：管理员需要手动在 new-api 后台修改用户余额
3. **定时同步**：需要配置 cron 任务定时调用同步 API
4. **Session 密钥**：必须与 new-api 的 session 密钥保持一致

## 开发

```bash
# 检查代码
cargo check

# 运行测试
cargo test

# 格式化代码
cargo fmt

# 代码检查
cargo clippy
```

## License

MIT
