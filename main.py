"""
Agnes 双企业账号本地中转（透明转发版）
====================================
设计原则
--------
- **透明转发**：原样转发任意方法 / 路径 / 请求体，支持 Agnes 的三种协议
  （/v1/chat/completions、/v1/messages、/v1/responses）以及其它任意 /v1/* 端点。
- **不解析、不改写请求体**：model 名、协议选择完全由客户端决定，中继不动。
- 仅做两件事的增值：
  1) 按令牌桶对每个 key 做 RPM 限流（主动预防）；
  2) 上游返回 429 时冷却该 key 并切换到下一个可用 key（故障转移）。
- 转发时只替换鉴权头为所选 key 的 Bearer（兼顾 Anthropic 协议的 x-api-key）。

部署
----
1. pip install -r requirements.txt
2. 编辑 config.toml（推荐，密钥与所有参数都在这里；或用环境变量覆盖，见 CONFIG）
3. 启动任选其一：
   - python main.py                         （读取 config.toml，默认 0.0.0.0:8000）
   - uvicorn main:app --host 0.0.0.0 --port 8000
4. 客户端：
   - 推荐用 OpenAI 兼容端点：base_url = http://localhost:8000/v1
     （Agnes 官方 API 即 OpenAI 兼容，/v1/chat/completions 对所有模型均可用，含 agnes-3.0-flash）
   - Claude / Anthropic 系：ANTHROPIC_BASE_URL = http://localhost:8000
     注意：上游的 /v1/messages（Anthropic 协议）对部分模型（如 agnes-3.0-flash）
     会挂起无响应，务必优先使用上面的 OpenAI 兼容端点。
   模型名直接用 Agnes 的（如 agnes-2.5-pro / agnes-2.5-flash / agnes-2.0-flash）；
   agnes-3.0-flash 在 /v1/chat/completions 上可用，但在 /v1/messages 上会挂起。
5. 运维端点：
   - GET /healthz          存活/就绪探针（可服务=200，降级/故障=503）
   - GET /status           富状态（含 alerts 告警评估）
   - GET /metrics          Prometheus 指标（含 agnes_relay_health / agnes_relay_alert）
   - 事件日志落地 logs/relay_events.log（可用 AGNES_LOG 覆盖）
6. 告警阈值可经环境变量微调：AGNES_ALERT_SUCCESS_RATE / _5XX_RATE / _429_RATE /
   _P95_S / _WINDOW_S / _MIN_SAMPLES（详见 CONFIG 段）。

注意：两个 key 必须属于两个独立企业账号，否则按文档共享同一限制池，切换无意义。
"""

import os
import time
import json
import tomllib
import asyncio
from collections import deque, defaultdict
from datetime import datetime
import httpx
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse, Response

# ----------------------------- CONFIG -----------------------------
# 配置来源优先级：环境变量 > config.toml > 内置默认值。
# 推荐直接编辑仓库根目录的 config.toml（改完重启即可），无需每次敲环境变量。
_HERE = os.path.dirname(os.path.abspath(__file__))


def _load_toml() -> dict:
    """读取同目录下的 config.toml；缺失或解析失败则回退为空字典（退回默认值）。"""
    path = os.path.join(_HERE, "config.toml")
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:  # 配置损坏不应阻断启动，退回默认值
        print(f"[warn] 读取 config.toml 失败，使用默认值: {e}")
        return {}


_CONFIG = _load_toml()


def _cfg(section: str, key: str, env: str, default):
    """取值优先级：环境变量 > config.toml[section][key] > default。"""
    env_val = os.getenv(env)
    if env_val is not None:
        return env_val
    return _CONFIG.get(section, {}).get(key, default)


# 注意：填域名根（不带 /v1）。中继会原样转发客户端发来的完整路径（含 /v1/...）。
UPSTREAM_BASE_URL = _cfg("upstream", "base_url", "AGNES_BASE_URL", "https://api.agnes-ai.cn")

# 两个独立企业账号的 key（只填你要在中继里用的；另一个你留着直连即可）
RAW_KEYS = [_cfg("upstream", "key_1", "AGNES_KEY_1", None),
            _cfg("upstream", "key_2", "AGNES_KEY_2", None)]

# 每个 key 的 RPM 上限（建议低于企业实际值留余量；企业文本实际约 40）
RAW_RPMS = [_cfg("limits", "rpm_1", "AGNES_RPM_1", "35"),
            _cfg("limits", "rpm_2", "AGNES_RPM_2", "35")]

# 单个 key 触发 429 后的冷却时长（秒），对齐 RPM 重置窗口
COOLDOWN_SECONDS = float(_cfg("limits", "cooldown_seconds", "AGNES_COOLDOWN", "60"))
# 上游请求超时（秒）
CLIENT_TIMEOUT = float(_cfg("limits", "timeout_seconds", "AGNES_TIMEOUT", "120"))

# 事件日志落地路径（默认 logs/relay_events.log，可在 config.toml[observability] 或 AGNES_LOG 覆盖）
LOG_PATH = os.getenv("AGNES_LOG", _CONFIG.get("observability", {}).get("log_path", "logs/relay_events.log"))
if not os.path.isabs(LOG_PATH):
    LOG_PATH = os.path.join(_HERE, LOG_PATH)

# ------------------------- 异常告警阈值 -------------------------
# 基于最近 ALERT_WINDOW_S 秒的请求结果滑动窗口评估，超过阈值即激活告警。
# 阈值均可经环境变量调小/调大，便于你正式运行时按实际表现微调。
ALERT_WINDOW_S = int(_cfg("observability", "alert_window_s", "AGNES_ALERT_WINDOW_S", "120"))      # 评估窗口（秒）
ALERT_MIN_SAMPLES = int(_cfg("observability", "alert_min_samples", "AGNES_ALERT_MIN_SAMPLES", "20"))  # 窗口样本不足不告警
ALERT_SUCCESS_RATE = float(_cfg("observability", "alert_success_rate", "AGNES_ALERT_SUCCESS_RATE", "0.90"))  # 成功率低于此值=critical
ALERT_5XX_RATE = float(_cfg("observability", "alert_5xx_rate", "AGNES_ALERT_5XX_RATE", "0.25"))   # 5xx 占比高于此值=warning
ALERT_429_RATE = float(_cfg("observability", "alert_429_rate", "AGNES_ALERT_429_RATE", "0.40"))   # 429 占比高于此值=warning（逼近限流）
ALERT_P95_S = float(_cfg("observability", "alert_p95_s", "AGNES_ALERT_P95_S", "45.0"))         # p95 延迟高于此秒数=warning
# ------------------------------------------------------------------


class TokenBucket:
    """异步安全的令牌桶，用于按 RPM 主动限流。"""

    def __init__(self, rpm: int):
        self.capacity = float(rpm)
        self.tokens = float(rpm)          # 启动即满桶
        self.refill = float(rpm) / 60.0   # 每秒补充的令牌数
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self.lock:
            now = time.monotonic()
            self.tokens = min(
                self.capacity,
                self.tokens + (now - self.last) * self.refill,
            )
            self.last = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False

    def available(self) -> float:
        now = time.monotonic()
        return min(
            self.capacity,
            self.tokens + (now - self.last) * self.refill,
        )


class Upstream:
    def __init__(self, name: str, api_key: str, rpm: int):
        self.name = name
        self.api_key = api_key
        self.bucket = TokenBucket(rpm)
        self.cooldown_until = 0.0
        self.last_used = 0.0  # 用于负载均衡（最久未使用优先）

    def cooling(self) -> bool:
        return time.monotonic() < self.cooldown_until

    def cooldown(self, seconds: float | None = None):
        self.cooldown_until = time.monotonic() + (seconds or COOLDOWN_SECONDS)


def build_pool() -> list[Upstream]:
    pool: list[Upstream] = []
    for i, (k, r) in enumerate(zip(RAW_KEYS, RAW_RPMS), start=1):
        if not k:
            continue
        pool.append(Upstream(f"upstream-{i}", k, int(r)))
    return pool


POOL = build_pool()
if not POOL:
    raise RuntimeError("未配置任何 AGNES_KEY_1 / AGNES_KEY_2，无法启动。")

# connect 用较短超时：上游连不上时快速失败（避免单请求最长等待约 2×CLIENT_TIMEOUT），
# read 保留 CLIENT_TIMEOUT 以支持长流式生成；write/pool 取适中值。
# local_address="0.0.0.0" 强制走 IPv4：本机存在 IPv6 隧道/VPN 适配器时，httpx 会优先
# 走 IPv6 而被隧道黑洞（连得上、收不到响应 → ReadTimeout）。强制 IPv4 可稳定连通。
aclient = httpx.AsyncClient(
    timeout=httpx.Timeout(connect=8.0, read=CLIENT_TIMEOUT, write=30.0, pool=8.0),
    transport=httpx.AsyncHTTPTransport(local_address="0.0.0.0"),
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 启动逻辑（如有）写在这里、yield 之前
    yield
    # 关闭逻辑：等价于原 on_event("shutdown")
    await aclient.aclose()


app = FastAPI(
    title="Agnes Relay",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)


# ----------------------------- 监控 / 遥测 -----------------------------
class _C:
    """单个计数器（上游维度或模型维度共用）。"""
    __slots__ = ("requests", "success", "upstream_429", "upstream_5xx",
                 "http_error", "cooldown", "failover", "stream_error",
                 "lat_sum", "lat_n", "lat_max", "lat_deque")

    def __init__(self):
        self.requests = 0
        self.success = 0
        self.upstream_429 = 0
        self.upstream_5xx = 0
        self.http_error = 0
        self.cooldown = 0
        self.failover = 0
        self.stream_error = 0
        self.lat_sum = 0.0
        self.lat_n = 0
        self.lat_max = 0.0
        self.lat_deque = deque(maxlen=1000)  # 最近 1000 条延迟，用于 p95/p99


class Metrics:
    """进程内轻量遥测（单事件循环，无需锁）。
    目的：正式运行后回采「未来可能要改的问题」——
    哪个 key 在悄悄被限、故障转移是否生效、延迟是否变高、某模型是否开始报错。"""

    def __init__(self):
        self.global_ = _C()
        self.per_upstream = defaultdict(_C)
        self.per_model = defaultdict(_C)
        self.events = deque(maxlen=400)          # 最近事件流水
        self.win = deque()                        # 最近请求结果滑动窗（告警速率用）
        self._alert_active = set()                # 当前激活告警类型（状态翻转日志用）
        self.started = time.monotonic()
        self._logpath = LOG_PATH
        try:
            os.makedirs(os.path.dirname(self._logpath), exist_ok=True)
            self._logf = open(self._logpath, "a", encoding="utf-8", buffering=1)
        except Exception:
            self._logf = None
        self.log_event("startup", "-", f"upstreams={[u.name for u in POOL]}")

    # --- 计数接口 ---
    def client_request(self, model):
        self.global_.requests += 1
        if model:
            self.per_model[model].requests += 1

    def attempt(self, up):
        self.per_upstream[up].requests += 1

    def success(self, up, model, latency):
        g, c = self.global_, self.per_upstream[up]
        g.success += 1; c.success += 1
        g.lat_sum += latency; g.lat_n += 1
        c.lat_sum += latency; c.lat_n += 1
        g.lat_deque.append(latency); c.lat_deque.append(latency)
        if latency > g.lat_max: g.lat_max = latency
        if latency > c.lat_max: c.lat_max = latency
        if model:
            m = self.per_model[model]
            m.success += 1; m.lat_sum += latency; m.lat_n += 1
            m.lat_deque.append(latency)
            if latency > m.lat_max: m.lat_max = latency

    def upstream_429(self, up, model):
        self.global_.upstream_429 += 1
        self.per_upstream[up].upstream_429 += 1
        if model:
            self.per_model[model].upstream_429 += 1

    def upstream_5xx(self, up, model):
        self.global_.upstream_5xx += 1
        self.per_upstream[up].upstream_5xx += 1
        if model:
            self.per_model[model].upstream_5xx += 1

    def http_error(self, up, model):
        self.global_.http_error += 1
        self.per_upstream[up].http_error += 1
        if model:
            self.per_model[model].http_error += 1

    def cooldown(self, up):
        self.global_.cooldown += 1
        self.per_upstream[up].cooldown += 1

    def failover(self):
        self.global_.failover += 1

    def stream_error(self, up, model):
        """流式转发过程中上游异常（如 ReadTimeout 挂起）的计数。"""
        self.global_.stream_error += 1
        self.per_upstream[up].stream_error += 1
        if model:
            self.per_model[model].stream_error += 1

    def log_event(self, kind, up, detail=""):
        ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        self.events.append({"ts": ts, "kind": kind, "up": up, "detail": detail[:200]})
        if self._logf:
            try:
                self._logf.write(f"{ts}\t{kind}\t{up}\t{detail[:200]}\n")
            except Exception:
                pass

    def note_outcome(self, ok: bool, kind: str, latency: float):
        """记录一次客户端请求的最终结果（成功/失败归类），供滑动窗口告警使用。
        kind ∈ {"ok","5xx","429","err"}；latency 为该请求端到端耗时（秒）。"""
        self.win.append((time.monotonic(), ok, kind, latency))

    def evaluate_alerts(self) -> dict:
        """基于最近 ALERT_WINDOW_S 秒的请求结果窗口评估异常告警。
        返回结构化字典，含激活告警列表、关键比率与阈值，供 /status、/metrics、/healthz 共用。
        窗口统一在此修剪；告警状态翻转（升起/恢复）记入事件日志。"""
        now = time.monotonic()
        cutoff = now - ALERT_WINDOW_S
        while self.win and self.win[0][0] < cutoff:
            self.win.popleft()
        n = len(self.win)
        alerts: list[dict] = []
        rates = {"success": None, "429": None, "5xx": None, "p95": None}

        if n >= ALERT_MIN_SAMPLES:
            ok = sum(1 for _, o, _, _ in self.win if o)
            c429 = sum(1 for _, _, k, _ in self.win if k == "429")
            c5xx = sum(1 for _, _, k, _ in self.win if k == "5xx")
            sr = ok / n
            r429 = c429 / n
            r5xx = c5xx / n
            lats = sorted(lt for _, o, _, lt in self.win if o and lt > 0)
            p95 = _pct(lats, 95) if lats else 0.0
            rates = {"success": round(sr, 3), "429": round(r429, 3),
                     "5xx": round(r5xx, 3), "p95": round(p95, 3)}
            if sr < ALERT_SUCCESS_RATE:
                alerts.append({"type": "low_success_rate", "severity": "critical",
                               "detail": f"成功率 {sr:.1%} < {ALERT_SUCCESS_RATE:.0%}（窗口 {n} 样本）"})
            if r5xx > ALERT_5XX_RATE:
                alerts.append({"type": "high_5xx_rate", "severity": "warning",
                               "detail": f"上游 5xx 占比 {r5xx:.1%} > {ALERT_5XX_RATE:.0%}"})
            if r429 > ALERT_429_RATE:
                alerts.append({"type": "high_429_rate", "severity": "warning",
                               "detail": f"上游 429 占比 {r429:.1%} > {ALERT_429_RATE:.0%}（逼近限流）"})
            if p95 > ALERT_P95_S:
                alerts.append({"type": "high_p95_latency", "severity": "warning",
                               "detail": f"p95 延迟 {p95:.1f}s > {ALERT_P95_S:.0f}s"})

        # 降级（故障转移）探测：任一上游正在冷却即视为降级（info，不计入 critical）
        cooling = [u.name for u in POOL if u.cooling()]
        if cooling:
            alerts.append({"type": "degraded_failover", "severity": "info",
                           "detail": f"上游冷却中（故障转移生效）：{cooling}"})

        # 状态翻转日志（仅 critical/warning，避免 info 刷屏）
        new_set = {a["type"] for a in alerts if a["severity"] in ("critical", "warning")}
        for t in new_set - self._alert_active:
            self.log_event("alert_raised", t,
                           next(a["detail"] for a in alerts if a["type"] == t))
        for t in self._alert_active - new_set:
            self.log_event("alert_cleared", t, "recovered")
        self._alert_active = new_set

        return {
            "window_samples": n,
            "min_samples": ALERT_MIN_SAMPLES,
            "rates": rates,
            "active": alerts,
            "thresholds": {
                "success_rate": ALERT_SUCCESS_RATE,
                "rate_429": ALERT_429_RATE,
                "rate_5xx": ALERT_5XX_RATE,
                "p95_s": ALERT_P95_S,
                "window_s": ALERT_WINDOW_S,
            },
        }


M = Metrics()


def _extract_model(raw: bytes):
    """从请求体尝试解析 model 名，用于按模型维度统计（解析失败返回 None）。"""
    try:
        obj = json.loads(raw)
        m = obj.get("model")
        return m if isinstance(m, str) else None
    except Exception:
        return None


def _pct(dq, p):
    """从滑动窗口计算百分位延迟（秒）。"""
    if not dq:
        return 0.0
    s = sorted(dq)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def _counter_view(c: _C) -> dict:
    avg = (c.lat_sum / c.lat_n) if c.lat_n else 0.0
    return {
        "requests": c.requests,
        "success": c.success,
        "upstream_429": c.upstream_429,
        "upstream_5xx": c.upstream_5xx,
        "http_error": c.http_error,
        "cooldown": c.cooldown,
        "failover": c.failover,
        "stream_error": c.stream_error,
        "latency_avg_s": round(avg, 3),
        "latency_max_s": round(c.lat_max, 3),
        "latency_p95_s": round(_pct(c.lat_deque, 95), 3),
        "latency_p99_s": round(_pct(c.lat_deque, 99), 3),
    }


def parse_retry_after(resp: httpx.Response) -> float | None:
    ra = resp.headers.get("retry-after")
    if ra and ra.isdigit():
        return float(ra)
    return None


async def select_upstream(exclude: set, allow_cooling: bool = False) -> Upstream | None:
    """挑选上游：优先未冷却者，同档下选「最久未使用」以实现多 key 负载均衡
    （避免旧逻辑总把流量压在 upstream-1，导致单 key 被打满后雪崩式冷却）。
    allow_cooling=True 时允许返回正在冷却的上游（用于全冷却场景，不再人为阻断，
    而是把上游真实 429 透传下去）。"""
    candidates = [
        u for u in POOL
        if id(u) not in exclude and (not u.cooling() or allow_cooling)
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda u: (u.cooling(), u.last_used))
    for u in candidates:
        if await u.bucket.acquire():
            return u
    return None


async def stream_generator(resp: httpx.Response, up: Upstream, model):
    """流式透传上游响应。

    修复点：上游在流式过程中可能“挂起”（长时间不发送下一 chunk），触发 httpx 的
    ReadTimeout。此前该异常在生成器内未被捕获，会一路冒泡到 ASGI 层，表现为
    “Exception in ASGI application” 并导致客户端断流。

    现在就地捕获、优雅收尾：记录告警事件、累加流错误计数、关闭上游连接，并在 SSE
    流末尾补发一个 error 事件，让客户端感知“异常结束”而非静默截断。"""
    bytes_sent = 0
    try:
        async for chunk in resp.aiter_bytes():
            bytes_sent += len(chunk)
            yield chunk
    except httpx.ReadTimeout:
        M.log_event("stream_read_timeout", up.name,
                    f"model={model} bytes_sent={bytes_sent}")
        M.stream_error(up.name, model)
        # SSE 流：补发结构化错误事件，避免客户端静默断流
        if "text/event-stream" in resp.headers.get("content-type", ""):
            try:
                yield (
                    b'data: {"type":"error","error":{"type":"upstream_stream_timeout",'
                    b'"message":"upstream stalled mid-stream (read timeout)"}}\n\n'
                )
            except Exception:
                pass
    except httpx.HTTPError as e:
        M.log_event("stream_http_error", up.name,
                    f"model={model} {type(e).__name__}: {e}")
        M.stream_error(up.name, model)
    finally:
        await resp.aclose()


def _forward_headers(request: Request, api_key: str) -> dict:
    """透传除控制类/鉴权类以外的请求头，仅注入真实 key 的 Bearer（与 x-api-key
    兼顾 chat/responses 与 messages 两种协议）。不改动任何业务头与请求体。"""
    headers: dict[str, str] = {}
    for k, v in request.headers.items():
        if k.lower() in (
            "host", "content-length", "connection",
            "accept-encoding", "authorization", "x-api-key",
        ):
            continue
        headers[k] = v
    headers["Authorization"] = f"Bearer {api_key}"
    headers["x-api-key"] = api_key
    if "anthropic-version" not in headers:
        headers["anthropic-version"] = "2023-06-01"
    # 强制上游不压缩：避免中继 httpx 自动解压后，仍把 content-encoding 头
    # 透传给下游客户端，导致"标称 gzip、实为明文"的解析失败。
    headers["accept-encoding"] = "identity"
    return headers


def _normalize_models(raw: dict) -> dict:
    """把 Agnes 非标 /v1/models 响应整形成 OpenAI 兼容结构。

    Agnes 原生：{data:[{id,created_at,display_name,type}], first_id, has_more, last_id}
    OpenAI 标准：{object:"list", data:[{id, object:"model", created, owned_by}]}
    严格的 OpenAI SDK 会因缺少 object 字段校验失败，故仅在此元数据端点做兼容整形，
    推理端点仍保持纯透明转发。"""
    items = raw.get("data", []) if isinstance(raw, dict) else []
    data = []
    for m in items:
        ca = m.get("created_at")
        created = 0
        if ca:
            try:
                created = int(datetime.fromisoformat(ca.replace("Z", "+00:00")).timestamp())
            except Exception:
                created = 0
        data.append({
            "id": m.get("id"),
            "object": "model",
            "created": created,
            "owned_by": "agnes",
        })
    return {"object": "list", "data": data}


async def _handle(request: Request):
    """通用透明转发：原样转发任意方法/路径/请求体，仅做双 key 故障转移与 RPM 限流。
    请求体（含 model 名）一律不解析、不改写。唯一例外：GET /v1/models（含裸 /models
    别名）按 OpenAI 结构整形，确保模型列表对兼容客户端可用。"""
    raw = await request.body()
    model = _extract_model(raw)

    # 模型列表兼容处理（仅 GET；裸 /models 别名映射到上游 /v1/models）
    path = request.url.path.rstrip("/")
    is_models = request.method == "GET" and path in ("/v1/models", "/models")
    fwd_path = "/v1/models" if path == "/models" else request.url.path
    target = f"{UPSTREAM_BASE_URL}{fwd_path}"
    if request.url.query:
        target += f"?{request.url.query}"

    M.client_request(model)
    exclude: set = set()
    last_detail = "all upstreams rate-limited or unavailable"
    last_429 = None  # (content_type, body, retry_after) —— 记录上游真实 429 以便透传
    transport_failed = False  # 是否发生过网络层（连接/DNS/代理）失败

    while True:
        u = await select_upstream(exclude)
        if u is None:
            # 全部冷却 / 本地限流：退而求其次，尝试最近将过期的上游（不人为阻断，
            # 仍把上游真实 429 透传下去）
            u = await select_upstream(exclude, allow_cooling=True)
            if u is None:
                break
        if exclude:  # 已进入重试（故障转移）
            M.failover()
        u.last_used = time.monotonic()
        exclude.add(id(u))
        M.attempt(u.name)

        headers = _forward_headers(request, u.api_key)
        t0 = time.monotonic()
        try:
            req = aclient.build_request(
                request.method, target, content=raw, headers=headers
            )
            resp = await aclient.send(req, stream=True)
        except httpx.HTTPError as e:
            # 网络层错误（连接/超时/DNS/代理等）：短暂冷却该上游，尝试下一个
            transport_failed = True
            u.cooldown(COOLDOWN_SECONDS)
            M.http_error(u.name, model)
            M.cooldown(u.name)
            err_msg = f"{type(e).__name__}: {e}"
            M.log_event("http_error", u.name, err_msg[:200])
            last_detail = f"{u.name} 连接上游失败（{err_msg}）"
            continue

        if resp.status_code == 429:
            ra = parse_retry_after(resp)
            detail = await resp.aread()
            u.cooldown(ra)
            M.upstream_429(u.name, model)
            M.cooldown(u.name)
            extra = detail.decode("utf-8", "ignore") if detail else ""
            M.log_event("upstream_429", u.name, extra[:120])
            # 记录上游真实 429，循环耗尽后原样透传给客户端（避免中继自己伪造 429
            # 造成比直连更差的可用性；共享限额池场景下尤为关键）。
            last_429 = (resp.headers.get("content-type", "application/json"), detail, ra)
            await resp.aclose()
            continue

        if resp.status_code != 200:
            # 非 429 的业务错误：原样透传，不再重试
            detail = await resp.aread()
            M.upstream_5xx(u.name, model)
            err_body = detail.decode("utf-8", "ignore") if detail else ""
            # 记录上游真实错误体（截断），否则 4xx/5xx 的精确原因无法排查
            # （例如 400 究竟是参数越界还是模型在该企业分组不可用）。
            M.log_event("upstream_error", u.name,
                        f"HTTP {resp.status_code} | {err_body[:300]}")
            M.note_outcome(False, "5xx", time.monotonic() - t0)
            h = {"Content-Type": resp.headers.get("content-type", "application/json")}
            await resp.aclose()
            return Response(content=detail, status_code=resp.status_code, headers=h)

        # 成功：原样透传上游响应（流式/非流式均由上游 content-type 决定）
        if is_models and "json" in resp.headers.get("content-type", ""):
            # 模型列表：整形成 OpenAI 兼容结构后返回（不影响推理端点透明性）
            body = await resp.aread()
            await resp.aclose()
            try:
                norm = _normalize_models(json.loads(body))
            except Exception:
                norm = {"object": "list", "data": []}
            M.success(u.name, model, time.monotonic() - t0)
            M.note_outcome(True, "ok", time.monotonic() - t0)
            return JSONResponse(norm, headers={"X-Upstream": u.name})

        lat = time.monotonic() - t0
        M.success(u.name, model, lat)
        M.note_outcome(True, "ok", lat)
        return StreamingResponse(
            stream_generator(resp, u, model),
            media_type=resp.headers.get("content-type", "application/octet-stream"),
            headers={
                **{
                    k: v for k, v in resp.headers.items()
                    if k.lower() not in ("transfer-encoding", "connection")
                },
                "X-Upstream": u.name,
            },
        )

    if last_429 is not None:
        # 上游真实 429：原样透传（含上游的 Retry-After），让客户端看到真实的限流信号
        ct, body, ra = last_429
        h = {"Content-Type": ct}
        if ra:
            h["Retry-After"] = str(int(ra))
        M.log_event("client_429", "-", "all upstreams rate-limited (last upstream 429 forwarded)")
        M.note_outcome(False, "429", time.monotonic() - t0)
        return Response(content=body, status_code=429, headers=h)

    if transport_failed:
        # 上游连不上（网络/DNS/代理/防火墙），语义上是网关错误，不是限流。
        # 返回 502 而非 429，避免客户端误判为自己被限流而空等或退避。
        M.log_event("client_502", "-", f"upstream unreachable: {last_detail}")
        M.note_outcome(False, "err", time.monotonic() - t0)
        return JSONResponse(
            status_code=502,
            content={
                "error": (
                    f"上游不可达：{last_detail}。"
                    f"请检查本机网络/代理/防火墙能否访问 {UPSTREAM_BASE_URL}"
                )
            },
        )

    M.log_event("client_429", "-", f"all upstreams cooling/unavailable: {last_detail}")
    M.note_outcome(False, "err", time.monotonic() - t0)
    return JSONResponse(
        status_code=429,
        content={"error": last_detail},
        headers={"Retry-After": str(int(COOLDOWN_SECONDS))},
    )


@app.get("/status")
async def status():
    """富状态：全局/每上游/每模型计数 + 最近事件流水，便于排查「某 key 被限 /
    故障转移没生效 / 延迟变高 / 某模型报错」等问题。"""
    return {
        "uptime_s": round(time.monotonic() - M.started, 1),
        "global": _counter_view(M.global_),
        "upstreams": [
            {
                "name": u.name,
                "cooling": u.cooling(),
                "available_tokens": round(u.bucket.available(), 2),
                **_counter_view(M.per_upstream[u.name]),
            }
            for u in POOL
        ],
        "by_model": {m: _counter_view(c) for m, c in M.per_model.items()},
        "alerts": M.evaluate_alerts(),
        "recent_events": list(M.events)[-30:],
    }


@app.get("/metrics")
async def metrics():
    """Prometheus 文本格式，可直接被 Prometheus/Grafana 抓取长期收集。"""
    g = M.global_
    L = []
    L.append("# HELP agnes_relay_requests_total 客户端请求总数")
    L.append("# TYPE agnes_relay_requests_total counter")
    L.append(f"agnes_relay_requests_total {g.requests}")
    L.append("# HELP agnes_relay_success_total 成功响应总数")
    L.append("# TYPE agnes_relay_success_total counter")
    L.append(f"agnes_relay_success_total {g.success}")
    L.append("# HELP agnes_relay_failover_total 故障转移触发次数")
    L.append("# TYPE agnes_relay_failover_total counter")
    L.append(f"agnes_relay_failover_total {g.failover}")

    def emit(name, help_, getter):
        L.append(f"# HELP {name} {help_}")
        L.append("# TYPE " + name.split("{")[0] + " counter")
        for u in POOL:
            L.append(f'{name}{{upstream="{u.name}"}} {getter(M.per_upstream[u.name])}')

    emit("agnes_relay_upstream_requests_total", "路由到该上游的尝试次数", lambda c: c.requests)
    emit("agnes_relay_upstream_success_total", "该上游成功次数", lambda c: c.success)
    emit("agnes_relay_upstream_429_total", "该上游返回 429 次数", lambda c: c.upstream_429)
    emit("agnes_relay_upstream_5xx_total", "该上游非200非429错误次数", lambda c: c.upstream_5xx)
    emit("agnes_relay_upstream_http_error_total", "该上游网络错误次数", lambda c: c.http_error)
    emit("agnes_relay_upstream_cooldown_total", "该上游被冷却次数", lambda c: c.cooldown)

    L.append("# HELP agnes_relay_latency_avg_seconds 成功响应平均延迟(秒)")
    L.append("# TYPE agnes_relay_latency_avg_seconds gauge")
    for u in POOL:
        c = M.per_upstream[u.name]
        avg = c.lat_sum / c.lat_n if c.lat_n else 0.0
        L.append(f'agnes_relay_latency_avg_seconds{{upstream="{u.name}"}} {avg:.4f}')
    L.append("# HELP agnes_relay_latency_max_seconds 成功响应最大延迟(秒)")
    L.append("# TYPE agnes_relay_latency_max_seconds gauge")
    for u in POOL:
        L.append(f'agnes_relay_latency_max_seconds{{upstream="{u.name}"}} {M.per_upstream[u.name].lat_max:.4f}')
    L.append("# HELP agnes_relay_latency_p95_seconds 成功响应 p95 延迟(秒, 最近1000样本)")
    L.append("# TYPE agnes_relay_latency_p95_seconds gauge")
    for u in POOL:
        L.append(f'agnes_relay_latency_p95_seconds{{upstream="{u.name}"}} {_pct(M.per_upstream[u.name].lat_deque, 95):.4f}')
    L.append("# HELP agnes_relay_latency_p99_seconds 成功响应 p99 延迟(秒, 最近1000样本)")
    L.append("# TYPE agnes_relay_latency_p99_seconds gauge")
    for u in POOL:
        L.append(f'agnes_relay_latency_p99_seconds{{upstream="{u.name}"}} {_pct(M.per_upstream[u.name].lat_deque, 99):.4f}')

    L.append("# HELP agnes_relay_model_requests_total 按模型的请求次数")
    L.append("# TYPE agnes_relay_model_requests_total counter")
    for m, c in M.per_model.items():
        L.append(f'agnes_relay_model_requests_total{{model="{m}"}} {c.requests}')

    # ---- 健康与告警 ----
    alerts = M.evaluate_alerts()
    ready = any((not u.cooling()) and u.bucket.available() > 0 for u in POOL)
    critical = any(a["severity"] == "critical" for a in alerts["active"])
    L.append("# HELP agnes_relay_health 中继健康状态(1=可服务,0=降级或故障)")
    L.append("# TYPE agnes_relay_health gauge")
    L.append(f"agnes_relay_health {0 if (not ready or critical) else 1}")
    L.append("# HELP agnes_relay_alerts_active 当前激活告警数量")
    L.append("# TYPE agnes_relay_alerts_active gauge")
    L.append(f"agnes_relay_alerts_active {len(alerts['active'])}")
    for atype in ("low_success_rate", "high_5xx_rate", "high_429_rate",
                  "high_p95_latency", "degraded_failover"):
        on = 1 if any(a["type"] == atype for a in alerts["active"]) else 0
        L.append(f'# HELP agnes_relay_alert{{type="{atype}"}} 告警是否激活(1/0)')
        L.append(f'agnes_relay_alert{{type="{atype}"}} {on}')

    return Response("\n".join(L) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/healthz")
async def healthz():
    """轻量存活/就绪探针，供负载均衡器或容器健康检查调用。

    判定：至少有一个上游「未冷却且仍有令牌」、且无 critical 告警时为 200（ok）；
    上游全部不可用但无 critical 告警时为 503（degraded）；存在 critical 告警时为 503（down）。
    body 仅含精简字段，适合高频探测。"""
    ready = any((not u.cooling()) and u.bucket.available() > 0 for u in POOL)
    alerts = M.evaluate_alerts()
    critical = any(a["severity"] == "critical" for a in alerts["active"])
    ok = ready and not critical
    return JSONResponse(
        status_code=200 if ok else 503,
        content={
            "status": "ok" if ok else ("degraded" if ready else "down"),
            "ready": ready,
            "upstreams": [
                {"name": u.name, "cooling": u.cooling(),
                 "available_tokens": round(u.bucket.available(), 2)}
                for u in POOL
            ],
            "alerts": [a["type"] for a in alerts["active"]],
            "window_samples": alerts["window_samples"],
        },
    )


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def proxy(request: Request):
    return await _handle(request)


if __name__ == "__main__":
    # 允许 `python main.py` 直接启动；host/port/log_level 取自 config.toml[server]
    # 或环境变量（AGNES_HOST / AGNES_PORT / AGNES_LOG_LEVEL），默认 0.0.0.0:8000。
    import uvicorn

    _host = _cfg("server", "host", "AGNES_HOST", "0.0.0.0")
    _port = int(_cfg("server", "port", "AGNES_PORT", "8000"))
    _lvl = _cfg("server", "log_level", "AGNES_LOG_LEVEL", "info")
    uvicorn.run(app, host=_host, port=_port, log_level=_lvl)
