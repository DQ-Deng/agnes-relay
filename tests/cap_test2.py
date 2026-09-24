"""
容量压测（终版）：修复负载均衡后，验证「双账号中继 ≈ 单账号 RPM 之和」
========================================================================
阶段A：K1 直连原生 60s 饱和 -> RPM_K1
阶段B：中继 双key 60s 饱和   -> RPM_relay
若 RPM_relay ≈ RPM_K1 × 2，则证明：两 key 是独立账号 + 中继修复后能真正叠加。
（K2 单账号 RPM 已在上轮测得 ≈ 42/min，此处用 K1 代表「另一个独立账号」。）
"""
import time
import threading
import httpx
from concurrent.futures import ThreadPoolExecutor

from cfg import load

UP, K1, K2 = load()  # 从仓库根 config.toml[upstream] 读取，脚本内不落 key
RL = "http://localhost:8000"
MODEL = "agnes-3.0-flash"
P = {"model": MODEL, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 8, "stream": False}


def sustained(url, key, duration=60, conc=15, label=""):
    headers = {"Authorization": f"Bearer {key}", "x-api-key": key, "Content-Type": "application/json"}
    stop = time.perf_counter() + duration
    lock = threading.Lock()
    c = {"200": 0, "429": 0, "other": 0, "err": 0}
    ups = {}

    def worker():
        while time.perf_counter() < stop:
            try:
                with httpx.Client() as cl:
                    r = cl.post(url + "/v1/chat/completions", json=P, headers=headers, timeout=30)
                    s = r.status_code
                    up = r.headers.get("X-Upstream", "-") if url == RL else "-"
            except Exception:
                s = None
            with lock:
                if s == 200:
                    c["200"] += 1
                    if up != "-":
                        ups[up] = ups.get(up, 0) + 1
                elif s == 429:
                    c["429"] += 1
                elif s is None:
                    c["err"] += 1
                else:
                    c["other"] += 1

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=conc) as ex:
        list(ex.map(lambda _: worker(), range(conc)))
    dt = time.perf_counter() - t0
    ok = c["200"]
    print(f"[{label}] 持续 {dt:.1f}s | 成功200={ok}  429={c['429']}  其它={c['other']}  错误={c['err']}")
    print(f"        估算 RPM ≈ {ok/dt*60:.0f}/min | X-Upstream 分布: {ups}")
    return ok


if __name__ == "__main__":
    print("等待 90s 让限速窗口重置 ...")
    time.sleep(90)

    print("\n>>> 阶段A：K1 直连原生 60s（测 K1 独立账号 RPM）")
    rpm_k1 = sustained(UP, K1, 60, 15, "K1-direct")

    print("\n>>> 等待 70s 重置 K1 窗口 ...")
    time.sleep(70)

    print("\n>>> 阶段B：中继 双key 60s（测两账号合计 RPM）")
    rpm_relay = sustained(RL, "relay-token", 60, 15, "relay-2key")

    print("\n" + "=" * 64)
    print("结论")
    print("=" * 64)
    print(f"K1 单账号 RPM ≈ {rpm_k1/60*60:.0f}/min（{rpm_k1} 成功/60s）")
    print(f"中继双账号 60s 成功 = {rpm_relay}")
    if rpm_relay >= rpm_k1 * 1.5:
        print(f"=> 中继 ≈ K1 的 {rpm_relay/max(rpm_k1,1):.2f}x，已明显叠加 -> 双独立账号 + 修复有效。")
    else:
        print("=> 中继未明显超过单账号，需进一步排查。")
