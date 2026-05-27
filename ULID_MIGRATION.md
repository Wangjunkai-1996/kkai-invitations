# ULID 支持迁移说明

## 概述

为返利记录添加了 ULID（Universally Unique Lexicographically Sortable Identifier）支持，确保每条记录有唯一标识。

## 变更内容

### 1. 依赖更新

在 `Cargo.toml` 中添加了 `ulid` crate：

```toml
ulid = "1.1"
```

### 2. 数据库表结构变更

`rebate_records` 表新增字段：

```sql
-- 添加 ulid 列
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS ulid VARCHAR(26);

-- 添加唯一索引
CREATE UNIQUE INDEX IF NOT EXISTS idx_rebate_records_ulid ON rebate_records(ulid);
```

**重要说明**：
- `ulid` 字段为 `VARCHAR(26)`，存储 ULID 的字符串表示
- 添加了唯一索引，确保 ULID 的唯一性
- 保留了原有的 `(order_type, order_id)` 唯一索引，继续作为防重复同步的机制

### 3. 代码变更

#### 3.1 同步逻辑（`src/routes/system.rs`）

在插入返利记录时生成 ULID：

```rust
// 生成 ULID
let ulid = ulid::Ulid::new().to_string();

// 插入返利记录
INSERT INTO rebate_records
(ulid, inviter_id, invitee_id, order_type, order_id, ...)
VALUES ($1, $2, $3, $4, $5, ...)
```

#### 3.2 API 响应结构（`src/routes/user.rs`）

`RebateRecord` 结构体新增 `ulid` 字段：

```rust
#[derive(Serialize)]
pub struct RebateRecord {
    pub id: i64,
    pub ulid: Option<String>,  // 新增
    pub order_type: String,    // API 输出为 orderType
    pub order_amount: i64,     // API 输出为 orderAmount，单位：人民币分
    pub rebate_amount: i64,    // API 输出为 rebateAmount，单位：人民币分
    pub rebate_ratio: f64,     // API 输出为 rebateRatio
    pub created_at: String,    // API 输出为 createdAt，ISO 时间
    pub status: String,
}
```

#### 3.3 查询更新

所有查询返利记录的 SQL 都已更新，包含 `ulid` 字段：

- `GET /api/invitations/rebate-records` - 用户查看返利记录
- `GET /api/invitations/available-rebates` - 用户查看可申请返利
- `GET /api/admin/user-rebates/:user_id` - 管理员查看用户返利明细

## 数据迁移

### 自动迁移

应用启动时会自动执行以下操作：

1. 检查 `rebate_records` 表是否存在 `ulid` 列
2. 如果不存在，自动添加该列
3. 创建唯一索引

### 现有数据处理

**现有记录的 ULID 字段为 NULL**：

- 新同步的记录会自动生成 ULID
- 历史记录的 `ulid` 字段保持为 `NULL`
- API 响应中 `ulid` 字段可为 `null`，兼容历史记录

如需为历史记录补充 ULID，可执行以下 SQL（可选）：

```sql
-- 为现有记录生成 ULID（需要在应用层执行）
-- 注意：ULID 生成需要在应用代码中完成，数据库无法直接生成
```

## API 响应示例

### 用户查看返利记录

```json
{
  "id": 123,
  "ulid": "01HQZX3Y4K5M6N7P8Q9R0S1T2U",
  "orderType": "topup",
  "orderAmount": 10000,
  "rebateAmount": 500,
  "rebateRatio": 0.05,
  "createdAt": "2025-01-01T00:00:00+00:00",
  "status": "pending"
}
```

### 历史记录（无 ULID）

```json
{
  "id": 100,
  "ulid": null,
  "orderType": "subscription",
  "orderAmount": 5000,
  "rebateAmount": 500,
  "rebateRatio": 0.1,
  "createdAt": "2024-12-31T00:00:00+00:00",
  "status": "completed"
}
```

## 兼容性

- **向后兼容**：现有 API 调用不受影响，`ulid` 字段为可选
- **数据库兼容**：自动迁移脚本确保平滑升级
- **防重复机制**：保留 `(order_type, order_id)` 唯一索引，不影响现有逻辑

## ULID 特性

- **长度**：26 字符（Base32 编码）
- **唯一性**：全局唯一，无需中心化协调
- **可排序**：按时间顺序排列
- **格式**：`01HQZX3Y4K5M6N7P8Q9R0S1T2U`
- **组成**：
  - 前 10 字符：时间戳（毫秒精度）
  - 后 16 字符：随机数

## 验证

运行以下命令验证实现：

```bash
# 编译检查
cargo check --locked

# 代码检查
cargo clippy --locked -- -D warnings

# 格式检查
cargo fmt --check

# 构建
cargo build --release --locked
```

所有检查均已通过 ✓

## 注意事项

1. **ULID 生成时机**：仅在同步新订单时生成，不会为历史记录补充
2. **唯一性保证**：通过数据库唯一索引确保 ULID 不重复
3. **防重复机制**：`(order_type, order_id)` 唯一索引继续作为主要防重复机制
4. **API 兼容性**：前端需要处理 `ulid` 字段可能为 `null` 的情况

## 后续优化建议

1. 考虑为历史记录批量生成 ULID（可选）
2. 在前端使用 ULID 作为记录的主要引用标识
3. 考虑在提现申请中使用 ULID 替代数据库 ID
