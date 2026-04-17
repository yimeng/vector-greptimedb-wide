# Vector GreptimeDB Wide Metrics 动态 dbname 与 table 使用说明

本 fork 基于 Vector 0.55.0，为 `greptimedb_wide_metrics` sink 增加了两个动态模板能力：

1. **`dbname`** — 从 `String` 改为 `Template`，支持从 metric tags/resource attributes 动态渲染
2. **`table`** — 新增 `Option<Template>`，支持每个 metric 独立动态表名

---

## 为什么需要这个 fork

Vector 原生的 `greptimedb_wide_metrics` sink 不支持动态 `dbname` 和 `table`。当你需要：

- 同一个 Vector 实例把不同任务/环境的 metric 写入不同的 GreptimeDB 数据库
- 按任务 ID 分表（如生产环境的 `ssw_399_aocs_tm` vs 测试环境的 `aocs_tm` 统一表）

时，就需要这个 fork。

---

## 快速开始

### Vector 配置示例

```toml
[sources.otel_in]
type = "opentelemetry"
address = "0.0.0.0:4317"

# 建议先用 remap 把 resource attributes 转成 tags
[transforms.prepare_tags]
type = "remap"
inputs = ["otel_in.metrics"]
source = '''
.tags.db_name = .tags."resource.db.name" || "public"
.tags.task_id = .tags."resource.task_instance_id" || "default"
'''

[sinks.greptime_out]
type = "greptimedb_wide_metrics"
inputs = ["prepare_tags"]
endpoint = "127.0.0.1:4001"

# 动态库名：会渲染成 "ssw_prod"、"yimeng_test" 等
dbname = "{{ tags.db_name }}"

# 动态表名：生成 "399_aocs_tm"、"default_aocs_tm" 等
table = "{{ tags.task_id }}_{{ metric.name }}"

# 白名单标签：这些 key 保留为 TAG，其余尝试解析为 f64 FIELD
tag_columns = ["task_instance_id", "task_id", "resource.service.name"]

# 当 tag 解析不是数字时的处理：tag/drop
fallback_behavior = "tag"

grpc_compression = "gzip"
```

### 对应的 OTel Collector 配置

```yaml
processors:
  transform/set_db_name:
    metric_statements:
      - context: metric
        statements:
          - set(resource.attributes["db.name"], "ssw_prod")
            where IsMatch(resource.attributes["env"], "prod")
          - set(resource.attributes["db.name"], "yimeng_test")
            where IsMatch(resource.attributes["env"], "test")
          - set(resource.attributes["task_instance_id"], "399")
            where IsMatch(resource.attributes["task_id"], "ssw_399")

exporters:
  otlp/vector:
    endpoint: vector-host:4317
    tls:
      insecure: true

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [transform/set_db_name, batch]
      exporters: [otlp/vector]
```

---

## 配置项详解

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `dbname` | `Template` | 数据库名，支持 `{{ tags.xxx }}` 语法。默认 `public` |
| `table` | `Option<Template>` | 可选表名模板。若配置则覆盖默认 `{ns}_{metric_name}` 格式 |
| `tag_columns` | `Vec<String>` | 白名单：这些 key 保留为 GreptimeDB TAG，不解析为数值 |
| `tag_column_patterns` | `Vec<String>` | 正则白名单，如 `^aocs_.*` |
| `fallback_behavior` | `Option<String>` | 当非白名单 tag 解析不是 f64 时的处理：`"tag"`(默认)、`"drop"` |

### 模板语法

- `{{ tags.db_name }}` — 从 metric 的 tags 中取值
- `{{ metric.name }}` — metric 名称
- `{{ tags.task_id }}_{{ metric.name }}` — 拼接，如 `399_aocs_tm`

> ⚠️ **重要**：Template 只能访问 `tags` 和 `metric.name`，不能直接访问 `resource.attributes`。如果需要使用 resource attributes，请先在 `remap` transform 或 OTel Collector 中转换成 tags。

---

## 表名逻辑

当 `table` 未配置时：
```
{namespace}_{metric_name}   # 如 ssw_test_aocs_tm
```

当 `table` 已配置时：
```
模板渲染结果               # 如 399_aocs_tm
```

如果模板渲染失败（如缺少必要 tag）：
- 发出 `TemplateRenderingError` 内部事件
- **不会丢弃 metric**
- 回退到默认的 `{ns}_{metric_name}` 规则

---

## 编译

### 在 rust-build 上 Docker 编译

```bash
ssh yimeng@198.18.100.181

sudo docker run --rm \
  -v /data/vector:/vector \
  -v /data/docker-vector-target:/vector/target \
  -w /vector \
  rust:1.75-bookworm \
  bash -c '
    export CARGO_HOME=/vector/target/cargo-home
    apt-get update && apt-get install -y protobuf-compiler libclang-dev libsasl2-dev
    cargo build --release --no-default-features \
      --features "api,unix,vrl/stdlib,sinks-greptimedb_wide_metrics,sinks-greptimedb_metrics,sinks-greptimedb_logs,sources-opentelemetry,transforms-remap,sinks-blackhole"
  '
```

产物：`/data/docker-vector-target/release/vector` (约 94MB)

### 特征列表

```
api,unix,vrl/stdlib,
sinks-greptimedb_wide_metrics,sinks-greptimedb_metrics,sinks-greptimedb_logs,
sources-opentelemetry,transforms-remap,sinks-blackhole
```

---

## 部署

```bash
# 从 rust-build 拷贝到目标节点
scp rust-build:/data/docker-vector-target/release/vector greptimedb4:/tmp/vector-new

# 替换并验证
ssh greptimedb4 "
  sudo cp /tmp/vector-new /usr/local/bin/vector
  sudo chmod +x /usr/local/bin/vector
  vector --version
  sudo systemctl restart vector
"
```

---

## 排查清单

1. **数据库必须存在** — GreptimeDB 不会自动建库：
   ```sql
   CREATE DATABASE IF NOT EXISTS ssw_prod;
   ```

2. **修改配置后必须 restart** — `reload` 不一定刷新 sink 配置：
   ```bash
   sudo systemctl restart vector
   ```

3. **Template 语法必须正确** — 使用 `tags.xxx` 而非 `resource.xxx`：
   ```toml
   # ✅ 正确
   dbname = "{{ tags.db_name }}"
   
   # ❌ 错误
   dbname = "{{ resource.db.name }}"
   ```

4. **先确保 tags 已经设置** — 在 OTel Collector 或 remap transform 中处理：
   ```toml
   [transforms.prepare_tags]
   type = "remap"
   source = '''
   .tags.db_name = .tags."resource.db.name" || "public"
   '''
   ```

---

## 与原版 Vector 的差异

| 功能 | 原版 Vector | 本 fork |
|------|-------------|---------|
| `dbname` | `String` 固定值 | `Template` 动态渲染 |
| `table` | 不存在 | `Option<Template>` 动态表名 |
| 默认表名 | `{ns}_{metric_name}` | 同左，或通过 `table` 覆盖 |
| 其他 sink | 无差异 | 无差异 |

---

## 常见问题

**Q: 为什么不改 Vector ，改在 OTel Collector 里拼接表名？**
A: 完全可行！如果你的需求只是按 `task_id` 分表，可以在 OTel Collector 的 `transform` 里直接修改 `metric.name`，Vector 维持固定配置。本 fork 的优势是 metric name 保持干净（始终是 `aocs_tm`），表名和 metric name 解耦。

**Q: 同一个 batch 里的 metric 可以有不同的表名吗？**
A: 可以。`GreptimeDBGrpcRequest` 内部包含的是 `RowInsertRequests` （多个 `RowInsertRequest` 的向量），每个 `RowInsertRequest` 有独立的 `table_name`。Vector 只按 `dbname` 分组，同一个 gRPC 请求可以包含多个表。

**Q: 会不会影响其他 sink？**
A: 不会。所有修改都限定在 `src/sinks/greptimedb/wide_metrics/` 目录下，其他 sink 不受影响。
