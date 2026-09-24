"""验证全局异常拦截 + 应用日志：临时注册一个必炸路由，经 TestClient 触发，
断言 1) 客户端拿到统一 JSON 500；2) 应用日志含完整 traceback；3) unhandled 计数与事件落账。"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import main
from fastapi.testclient import TestClient

failures = []

# 临时注册一个必炸路由（仅存在于本测试进程，测试后移除）。
# 注意：必须插到 routes 最前面——app 里有 catch-all `/{path:path}` 透明转发，
# 后注册的路由永远匹配不到（按注册顺序匹配）。
from fastapi.routing import APIRoute

def _boom():
    raise RuntimeError("boom-test-异常拦截自检")

main.app.router.routes.insert(0, APIRoute("/_test_boom", endpoint=_boom, methods=["GET"]))

c = TestClient(main.app, raise_server_exceptions=False)
before = main.M.global_.unhandled
r = c.get("/_test_boom")
print("响应:", r.status_code, r.headers.get("content-type"), "| body:", r.text[:120])

if r.status_code != 500:
    failures.append(f"期望 500，实得 {r.status_code}")
if "relay internal error" not in r.text or "RuntimeError" not in r.text:
    failures.append(f"响应体不是统一 JSON 500: {r.text[:120]}")

# 应用日志：应含本次 traceback
time.sleep(0.2)
applog_content = ""
for f in [main.APP_LOG_PATH] + [f"{main.APP_LOG_PATH}.{i}" for i in range(1, 4)]:
    if os.path.exists(f):
        applog_content += open(f, encoding="utf-8", errors="ignore").read()
if "boom-test" not in applog_content or "Traceback" not in applog_content:
    failures.append("应用日志中未找到该异常的 traceback")

# 计数与事件
if main.M.global_.unhandled != before + 1:
    failures.append(f"unhandled 计数未 +1（{before} -> {main.M.global_.unhandled}）")
events = [e for e in main.M.events if e["kind"] == "unhandled_error"]
if not events or "boom-test" not in events[-1]["detail"]:
    failures.append(f"事件流水缺少 unhandled_error: {events[-1] if events else None}")

# 移除临时路由
main.app.router.routes = [rt for rt in main.app.router.routes if getattr(rt, "path", None) != "/_test_boom"]

# /status 应能看到 unhandled 计数
import asyncio
st = asyncio.run(main.status())
print("/status global.unhandled =", st["global"]["unhandled"])
if st["global"]["unhandled"] != main.M.global_.unhandled:
    failures.append("/status unhandled 与 M 不一致")

print("\n" + ("PASS: 全局异常拦截三层记录全部生效" if not failures else "FAIL:\n  " + "\n  ".join(failures)))
sys.exit(1 if failures else 0)
