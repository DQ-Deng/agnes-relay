# Agnes Relay（双企业账号本地中转）

把本地流量透明转发到 Agnes 上游，仅做两件事的增值：

1. **按 key 令牌桶限流**（主动预防 429）；
2. **上游 429 时冷却该 key 并故障转移到下一个可用 key**。

请求体、model 名、协议选择一律**原样透传、不改写**，支持 Agnes 的三种协议
（`/v1/chat/completions`、`/v1/messages`、`/v1/responses`）以及其它任意 `/v1/*` 端点。
唯一例外：`GET /v1/models` 会整形成 OpenAI 标准结构，保证兼容客户端可用。

## 要求

- **Python ≥ 3.11**（代码使用了 3.11 标准库 `tomllib`）
- 两个**独立企业账号**的 API key。两个 key 若属同一账号会共享限制池，故障转移失去意义。

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 复制示例配置并填入你的两个 key
copy config.example.toml config.toml    # Windows
# cp config.example.toml config.toml    # Linux/macOS
# 编辑 config.toml：[upstream] 下的 key_1 / key_2

# 3. 启动（任选其一）
python main.py                          # 默认 0.0.0.0:8000
uvicorn main:app --host 0.0.0.0 --port 8000
```

未配置任何 key 时服务**拒绝启动**（报 `RuntimeError`），防止半残状态上线。

## 配置

配置来源优先级：**环境变量 > `config.toml` > 内置默认值**。改完 `config.toml` 重启即可。

`config.toml` 含密钥，已写入 `.gitignore`，**请勿提交**。各段说明见
[config.example.toml](config.example.toml) 内的注释。

主要环境变量（与 config 段一一对应）：

| 环境变量 | 作用 |
| --- | --- |
| `AGNES_BASE_URL` | 上游域名根（不带 `/v1`） |
| `AGNES_KEY_1` / `AGNES_KEY_2` | 两个企业账号的 key |
| `AGNES_RPM_1` / `AGNES_RPM_2` | 每 key 的 RPM 上限 |
| `AGNES_COOLDOWN` | 触发 429 后的冷却秒数 |
| `AGNES_TIMEOUT` | 上游请求超时（秒） |
| `AGNES_HOST` / `AGNES_PORT` / `AGNES_LOG_LEVEL` | 监听地址、端口、日志级别 |
| `AGNES_LOG` | 事件日志落地路径 |
| `AGNES_APP_LOG` | 应用日志（程序自身运维日志/异常 traceback）落地路径 |
| `AGNES_LOG_MAX_MB` / `AGNES_LOG_BACKUPS` | 日志滚动：单文件上限(MB) / 保留备份份数 |
| `AGNES_ALERT_*` | 告警阈值，见下表 |

## 客户端接入

- **推荐 OpenAI 兼容端点**：`base_url = http://localhost:8000/v1`
  （Agnes 官方 API 即 OpenAI 兼容，`/v1/chat/completions` 对所有模型可用，含 agnes-3.0-flash）
- Claude / Anthropic 系：`ANTHROPIC_BASE_URL = http://localhost:8000`
  ⚠️ 上游 `/v1/messages` 对部分模型（如 agnes-3.0-flash）会挂起无响应，务必优先用 OpenAI 兼容端点。

模型名直接用 Agnes 的：`agnes-2.5-pro` / `agnes-2.5-flash` / `agnes-2.0-flash` / `agnes-3.0-flash`。

## 运维端点

| 端点 | 用途 |
| --- | --- |
| `GET /healthz` | 存活/就绪探针（可服务=200，降级/故障=503），body 仅精简字段，适合高频探测 |
| `GET /status` | 富状态：全局/每上游/每模型计数、最近事件流水、告警评估 |
| `GET /metrics` | Prometheus 文本格式，含 `agnes_relay_health` / `agnes_relay_alert`，可被长期抓取 |

事件日志默认落地 `logs/relay_events.log`（可用 `AGNES_LOG` 覆盖）。**按大小自动滚动**：
单文件写满 `log_max_mb`（默认 10MB）即轮转为 `.1`、`.2` …（标准轮转命名），仅保留最近
`log_backups`（默认 5）份，最旧的自动丢弃；`log_max_mb = 0` 可关闭轮转。
参数在 `config.toml[observability]` 或 `AGNES_LOG_MAX_MB` / `AGNES_LOG_BACKUPS` 中配置；
每次轮转记一条 `log_rotated` 事件，累计次数与当前文件大小可在 `/status` 的 `log` 段看到。

## 应用日志与全局异常拦截

- 意外异常由**全局异常处理器**拦截（`/status`、`/metrics`、`/healthz` 及所有转发路径统一生效）：
  - 完整 traceback 写入**应用日志** `logs/relay_app.log`（与事件日志共用 `log_max_mb` / `log_backups` 滚动）
  - 事件日志记一条 `unhandled_error` 事件（进 `recent_events` 与流水文件）
  - 客户端收到统一 JSON 500（仅含异常类型名，不泄露内部细节）
- 没有它之前，意外异常只会以 uvicorn 控制台的 `Exception in ASGI application` 形式出现，
  不落盘、无管理、易被忽略。
- **边界**：流式响应**已开始**之后发生的异常，异常处理器无法再介入（响应头已发出），
  由 `stream_generator` 的生成器内捕获负责：记 `stream_read_timeout` / `stream_http_error` 事件，
  并在 SSE 流末尾补发结构化 error 事件，避免客户端静默断流。
- 相关计数：`/status` 全局维度的 `unhandled`、`/metrics` 的 `agnes_relay_unhandled_errors_total`。

## 异常告警

基于最近 `alert_window_s` 秒滑动窗口评估，超过阈值即在 `/status`、`/metrics`、`/healthz` 中激活告警，
状态翻转（升起/恢复）记入事件日志：

| 告警 | 类型 | 默认阈值 | 可调环境变量 |
| --- | --- | --- | --- |
| 成功率过低 | critical | < 0.90 | `AGNES_ALERT_SUCCESS_RATE` |
| 5xx 占比过高 | warning | > 0.25 | `AGNES_ALERT_5XX_RATE` |
| 429 占比过高 | warning | > 0.40 | `AGNES_ALERT_429_RATE` |
| p95 延迟过高 | warning | > 45s | `AGNES_ALERT_P95_S` |
| 任一上游冷却中 | info | — | — |

窗口长度 `AGNES_ALERT_WINDOW_S`（默认 120s）、最小样本数 `AGNES_ALERT_MIN_SAMPLES`（默认 20）。
样本不足时不评估成功率类告警。

## 429 / 502 语义

- **上游真实 429**：循环耗尽后**原样透传**给客户端（含上游 `Retry-After`），避免中继自己伪造 429
  造成比直连更差的可用性——共享限额池场景下尤其关键。
- **网络层故障**（连接/超时/DNS/代理失败）：返回 **502** 而非 429，语义是"上游不可达"，
  避免客户端误判为自己被限流而空等退避。

## 本地测试脚本（tests/）

需要服务已在 `localhost:8000` 运行：

| 脚本 | 用途 |
| --- | --- |
| `tests/verify.py` | 验证三种协议透传不改写（chat 非流/流、messages、responses） |
| `tests/bench.py` | 出 token 速度、并发压测对比直连与中继 |
| `tests/cap_test2.py` | 容量/并发探针 |
| `tests/diag3.py` | 快速连通性诊断 |

> 脚本内不含任何密钥：直连对比用的 key 统一由 `tests/cfg.py` 从仓库根的 `config.toml`
> 读取（该文件已被 `.gitignore` 排除）。运行前请确保 `config.toml` 已填好真实 key。
