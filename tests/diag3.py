"""
隔离诊断：低速率 + 全新窗口下，验证
1) 中继是否能正常成功（排除饱和死亡螺旋的干扰）
2) K1 / K2 各自直连是否都 200（确认两把 key 是否都是有效独立账号）
"""
import time
import httpx

from cfg import load

UP, K1, K2 = load()  # 从仓库根 config.toml[upstream] 读取，脚本内不落 key
RL = "http://localhost:8000"
P = {"model": "agnes-3.0-flash", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 8, "stream": False}


def post(url, key, label):
    h = {"Authorization": f"Bearer {key}", "x-api-key": key, "Content-Type": "application/json"}
    try:
        r = httpx.post(url + "/v1/chat/completions", json=P, headers=h, timeout=30)
        print(f"  {label}: {r.status_code}  X-Upstream={r.headers.get('X-Upstream')}  body={r.text[:120]}")
        return r.status_code
    except Exception as e:
        print(f"  {label}: ERR {e}")
        return None


print("等待 90s 让两个账号限速窗口重置 ...")
time.sleep(90)

print("\n[1] 中继 8 个低速率顺序请求（间隔 2s）:")
for i in range(8):
    post(RL, "relay-token", f"relay#{i+1}")
    time.sleep(2)

print("\n[2] K1 直连原生 单次:")
post(UP, K1, "K1-direct")
print("[3] K2 直连原生 单次:")
post(UP, K2, "K2-direct")
