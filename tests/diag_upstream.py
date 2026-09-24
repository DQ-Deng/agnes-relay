"""上游连通性诊断：排查"两个 key 同时挂"到底断在哪一层。

双 key 同挂几乎必然是共因故障（同一台机器 → 同一套网络栈 → 同一个上游域名），
本脚本按层定位：DNS → 本机出口(隧道/IPv6) → 逐地址 TCP → 逐地址 TLS → （可选）端到端探测。

用法：
    python tests/diag_upstream.py            # 网络层诊断，不需要 key
    python tests/diag_upstream.py --probe    # 另按 config.toml 的两个 key 各发 1 次最小请求
                                             # （stream=False, max_tokens=1，耗配额可忽略）
    python tests/diag_upstream.py --probe --model agnes-2.5-flash

决策参考（看"结论"段）：
    v4 TCP 都死                → 运营商 / 上游服务端问题，等待恢复或换出口
    v4 活、v6 黑洞             → IPv6 出口问题（隧道/VPN/运营商）；中继已钉死 IPv4，不受影响
    TCP 活、TLS 死             → 中间盒（防火墙/运营商深度检测）或上游挂起
    TCP+TLS 都活              → 路径健康，问题在业务层（key 失效/403、渠道 503、模型 404）
"""
import os
import re
import ssl
import sys
import time
import socket
import platform
import subprocess
from urllib.parse import urlparse

try:
    import tomllib
except ImportError:  # py<3.11
    import tomli as tomllib

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def _reconfigure_utf8():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def load_base_url() -> str:
    """只读 base_url（不需要 key）；config.toml 缺失/无该段时用内置默认值。"""
    try:
        with open(os.path.join(ROOT, "config.toml"), "rb") as f:
            return tomllib.load(f).get("upstream", {}).get("base_url") or "https://api.agnes-ai.cn"
    except Exception:
        return "https://api.agnes-ai.cn"


def tcp_probe(addr: str, port: int, timeout: float = 4.0):
    """返回 (ok, 毫秒或错误信息)。区分'快速失败(路由/防火墙)'与'超时(黑洞)'。"""
    t0 = time.monotonic()
    try:
        s = socket.create_connection((addr, port), timeout=timeout)  # 族由地址串自动推断
        ms = (time.monotonic() - t0) * 1000
        s.close()
        return True, ms
    except OSError as e:
        return False, f"{type(e).__name__}: {e.strerror or e}"


def tls_probe(host: str, addr: str, port: int, timeout: float = 5.0):
    """在已验证 TCP 可达的地址上做 TLS 握手，区分'连上但无响应'的黑洞与正常链路。"""
    t0 = time.monotonic()
    try:
        raw = socket.create_connection((addr, port), timeout=timeout)
        ctx = ssl.create_default_context()
        with ctx.wrap_socket(raw, server_hostname=host) as s:
            ms = (time.monotonic() - t0) * 1000
            ver = s.version() or "TLS"  # 必须在 with 块内取（关闭后 version() 返回 None）
        return True, f"{ms:.0f}ms {ver}"
    except (OSError, ssl.SSLError) as e:
        return False, f"{type(e).__name__}: {e}"


def adapters() -> list[str]:
    """本机网络适配器概览（标记隧道/VPN 类）。"""
    win = platform.system() == "Windows"
    cmd = "ipconfig" if win else "ip addr"
    try:
        out = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10).stdout
    except Exception:
        return ["<无法获取适配器信息>"]
    names = []
    for ln in out.splitlines():
        ln = ln.strip()
        if win:
            if re.match(r"^[^ ].*适配器", ln) or re.match(r"^Ethernet \d", ln) or "adapter" in ln.lower():
                if ln and not ln.startswith("Media"):
                    names.append(ln)
        else:
            m = re.match(r"^\d+:\s+(\S+)", ln)
            if m and m.group(1) != "lo":
                names.append(m.group(1))
    marks = []
    for n in names:
        low = n.lower()
        tag = " [隧道/VPN!]" if any(k in low for k in ("tunnel", "isatap", "teredo", "vpn", "wireguard", "tailscale", "clash")) else ""
        marks.append(f"- {n}{tag}")
    return marks or ["<未识别到适配器>"]


def default_v6_route() -> str:
    win = platform.system() == "Windows"
    cmd = "route print ipv6" if win else "ip -6 route show default"
    try:
        out = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10).stdout
    except Exception:
        return "<无法获取>"
    for ln in out.splitlines():
        if ("0000:0000" in ln and "0000:0000" in ln) or ln.startswith("default"):
            return ln.strip()
    return "无默认 IPv6 路由"


def main():
    _reconfigure_utf8()
    base_url = load_base_url()
    host = urlparse(base_url).hostname
    port = int(urlparse(base_url).port or (443 if "https" in base_url else 80))
    print(f"目标: {base_url}  (host={host}, port={port})\n")

    # ---- 1) DNS：收集 v4/v6 全部地址 ----
    addrs: dict[int, list[str]] = {socket.AF_INET: [], socket.AF_INET6: []}
    try:
        for a in socket.getaddrinfo(host, port):
            fam, _, _, _, sa = a[0], a[1], a[2], a[3], a[4]
            ip = sa[0]
            if fam in addrs and ip not in addrs[fam]:
                addrs[fam].append(ip)
    except OSError as e:
        print(f"!! DNS 解析失败: {e}  ←  最底层就断了（DNS/网络整体故障）")
        return
    print("== 1) DNS ==")
    print(f"  v4: {addrs[socket.AF_INET] or '无 A 记录'}")
    print(f"  v6: {addrs[socket.AF_INET6] or '无 AAAA 记录'}\n")

    # ---- 2) 本机出口 ----
    print("== 2) 本机出口 ==")
    for m in adapters():
        print(m)
    print(f"  默认 v6 路由: {default_v6_route()}\n")

    # ---- 3) 逐地址 TCP + TLS ----
    print("== 3) 逐地址连通性（4s 超时，区分'快速失败'与'黑洞'） ==")
    results: list[tuple[str, bool, str, str]] = []  # (addr, tcp_ok, tcp_info, tls 描述)
    for fam, ips in ((socket.AF_INET, addrs[socket.AF_INET]), (socket.AF_INET6, addrs[socket.AF_INET6])):
        label = "v4" if fam == socket.AF_INET else "v6"
        for ip in ips:
            ok, info = tcp_probe(ip, port)
            if ok:
                tok, tinfo = tls_probe(host, ip, port)
                t = f"{('活' if tok else '死!') + tinfo}"
            else:
                t = "-"
            line = f"  {label} {ip:<39} TCP {'活 %.0fms' % info if ok else '死 ' + info} | TLS {t}"
            results.append((ip, ok, str(info), t))
            print(line)
    print()

    # ---- 4) 结论 ----
    v4 = [r for r in results if ":" not in r[0]]
    v6 = [r for r in results if ":" in r[0]]
    print("== 结论 ==")
    if not v4:
        print("  [v4 无]  DNS 未返回 A 记录 → 若 v6 也不可用则全层断。")
    elif all(not ok for _, ok, _, _ in v4):
        print("  [v4 全死]  v4 记录存在但全不可达 → 运营商/上游侧故障，等待恢复或换出口。")
    else:
        print("  [v4 有活]  IPv4 路径可用；中继已钉死 IPv4（local_address=0.0.0.0），不受 v6 影响。")
    for ip, ok, cinfo, t in v6:
        if not ok:
            reason = "连接超时（黑洞：包发出去了但收不回来）" if "timed out" in cinfo.lower() else "快速失败（无路由/防火墙）"
            print(f"  [v6 死] {ip}：{reason} → 本机 v6 出口不可用。"
                  f"已钉死 IPv4 的中继不受影响，但任何走 v6 的流量都会挂。")
        elif t.startswith("死"):
            print(f"  [v6 黑洞] {ip}：TCP 通但 TLS 死 → v6 出口被黑洞（隧道/VPN/运营商 v6 异常）。")
        else:
            print(f"  [v6 活]   {ip}：v4/v6 均可用。")
    if results and all(not ok for _, ok, _, _ in results):
        print("  [全层断]  所有地址 TCP 都不可达 → 本机出口整体故障（断网/路由/DNS），与两个 key 无关。")

    # ---- 5) 可选：端到端探测（每 key 1 次最小请求）----
    if "--probe" in sys.argv:
        import httpx
        from cfg import load  # tests/ 目录下直接可用
        model = "agnes-2.5-flash"
        if "--model" in sys.argv:
            model = sys.argv[sys.argv.index("--model") + 1]
        base, k1, k2 = load()
        print(f"\n== 4) 端到端探测（model={model}, max_tokens=1, 每 key 1 次） ==")
        c = httpx.Client(
            timeout=httpx.Timeout(connect=8.0, read=25.0, write=10.0, pool=5.0),
            transport=httpx.HTTPTransport(local_address="0.0.0.0"),  # 与中继同一钉死方式
        )
        for idx, key in ((1, k1), (2, k2)):
            body = {"model": model, "max_tokens": 1,
                   "messages": [{"role": "user", "content": "hi"}]}
            try:
                r = c.post(f"{base}/v1/chat/completions",
                           headers={"Authorization": f"Bearer {key}"}, json=body)
                print(f"  key_{idx}: HTTP {r.status_code} | {r.text[:160].strip()}")
            except Exception as e:
                print(f"  key_{idx}: {type(e).__name__}: {e}")
        c.close()
    else:
        print("\n提示: 加 --probe 可用 config.toml 的两个 key 各发 1 次最小请求，验证端到端。")


if __name__ == "__main__":
    main()
