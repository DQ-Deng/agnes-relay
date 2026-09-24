"""
中继对照基准测试
================
目的：客观衡量「单 key 直连原生」vs「双 key 故障转移中继」的差异。

维度1：出 token 速度 (tps)
  - 原生直连（单 key） vs 经中继（双 key），同一模型流式测速。
  - 中继只多一跳网络，tps 应几乎一致；若有差异也只是毫秒级代理开销。

维度2：并发承载（判断「双 key 是否真有改善」）
  - 突发 N 个并发小请求：
      * 原生直连单 key  -> 看多少被 429
      * 经中继双 key    -> 看多少被 200，且 X-Upstream 是否两个都用上
  - 若中继成功数≈单 key 的 2 倍且两个上游都被用 -> 两 key 属于独立账号池，
    你的方法带来 ~2x 并发余量与故障转移韧性。
  - 若中继成功数≈单 key -> 两 key 共享同一限额池，双 key 几乎无改善。

注意：本测试用 max_tokens 极小，仅探测 RPM/接受能力，不消耗多少额度。
"""
import time
import json
import statistics
from concurrent.futures import ThreadPoolExecutor
import httpx

from cfg import load

UPSTREAM, KEY1, KEY2 = load()  # 从仓库根 config.toml[upstream] 读取，脚本内不落 key
RELAY = "http://localhost:8000"
MODEL = "agnes-3.0-flash"

# ----------------------------------------------------------------------
# 维度1：出 token 速度
# ----------------------------------------------------------------------
def measure_tps(base_url, api_key, label, repeats=3):
    """非流式请求，用 usage.completion_tokens / 端到端耗时 估算 tps。
    原生与中继用同一算法，比较差值即为中继额外开销。"""
    tps_list = []
    for i in range(repeats):
        payload = {
            "model": MODEL,
            "messages": [{"role": "user", "content": "请详细介绍一下中国的四大发明，每段不少于两句话。"}],
            "max_tokens": 400,
            "stream": False,
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "x-api-key": api_key,
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client() as client:
                t0 = time.perf_counter()
                r = client.post(f"{base_url}/v1/chat/completions", json=payload,
                                headers=headers, timeout=60)
                t1 = time.perf_counter()
            if r.status_code != 200:
                print(f"  [{label}] 第{i+1}次 HTTP {r.status_code}: {r.text[:120]}")
                continue
            d = r.json()
            ct = d.get("usage", {}).get("completion_tokens")
            el = t1 - t0
            if ct and el > 0:
                tps = ct / el
                tps_list.append(tps)
                print(f"  [{label}] 第{i+1}次: tokens={ct} 端到端={el:.2f}s tps={tps:.1f}")
        except Exception as e:
            print(f"  [{label}] 第{i+1}次失败: {e}")
    if tps_list:
        print(f"  [{label}] tps 均值 = {statistics.mean(tps_list):.1f} (样本 {len(tps_list)})")
    return statistics.mean(tps_list) if tps_list else None


# ----------------------------------------------------------------------
# 维度2：并发承载（探测是否独立池）
# ----------------------------------------------------------------------
def burst(base_url, api_key, n, concurrency, label):
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 16,
        "stream": False,
    }
    headers = {"Authorization": f"Bearer {api_key}", "x-api-key": api_key,
               "Content-Type": "application/json"}

    def call(_):
        try:
            with httpx.Client() as client:
                r = client.post(f"{base_url}/v1/chat/completions", json=payload,
                                headers=headers, timeout=30)
                return r.status_code, r.headers.get("X-Upstream", "-")
        except Exception as e:
            return ("ERR", str(e)[:40])

    codes = []
    upstreams = {}
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        for code, up in ex.map(call, range(n)):
            codes.append(code)
            if up != "-":
                upstreams[up] = upstreams.get(up, 0) + 1
    dt = time.perf_counter() - t0
    ok = sum(1 for c in codes if c == 200)
    rl = sum(1 for c in codes if c == 429)
    print(f"  [{label}] 总={n} 并发={concurrency} 耗时={dt:.1f}s | 200={ok}  429={rl}  其它={n-ok-rl}")
    print(f"           X-Upstream 分布: {upstreams}")
    return ok, rl


if __name__ == "__main__":
    print("=" * 64)
    print("维度1：出 token 速度 (agnes-3.0-flash，max_tokens=400)")
    print("=" * 64)
    print("原生直连 (key#2 单 key):")
    tps_native = measure_tps(UPSTREAM, KEY2, "native")
    print("经中继 (双 key):")
    tps_relay = measure_tps(RELAY, "relay-proxy-token", "relay")
    if tps_native and tps_relay:
        diff = (tps_relay - tps_native) / tps_native * 100
        print(f"  => tps 差异: {diff:+.1f}%  (原生 {tps_native:.1f} vs 中继 {tps_relay:.1f})")

    print()
    print("=" * 64)
    print("维度2：并发承载 — 突发 60 请求，探测限额池是否独立")
    print("=" * 64)
    print("单 key 原生直连 (key#2):")
    ok_n, rl_n = burst(UPSTREAM, KEY2, 60, 30, "native-1key")
    print("双 key 经中继:")
    ok_r, rl_r = burst(RELAY, "relay-proxy-token", 60, 30, "relay-2key")

    print()
    print("=" * 64)
    print("结论判断")
    print("=" * 64)
    if ok_r >= ok_n * 1.5:
        print(f"中继成功数(200={ok_r}) ≈ 单key({ok_n})的 {ok_r/max(ok_n,1):.1f}x -> 两 key 大概率为【独立账号池】，")
        print("你的双 key 故障转移方法带来明显并发余量 + 韧性改善。")
    else:
        print(f"中继成功数(200={ok_r}) ≈ 单key({ok_n}) -> 两 key 可能为【共享限额池】，")
        print("双 key 几乎无并发改善；方法价值主要在偶发 429 时的自动切换韧性。")
