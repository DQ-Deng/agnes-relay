"""透明转发验证：依次验证三种 Agnes 协议，确认中继不改写 model / body。"""
import json
import httpx

BASE = "http://localhost:8000"
# 客户端无需任何鉴权头：真实 key 由中继按负载均衡注入，这里占位即可。


def status():
    r = httpx.get(f"{BASE}/status", timeout=10)
    print("== /status ==>", r.status_code, r.json())
    return r.status_code == 200


def chat_nonstream():
    print("\n== /v1/chat/completions (agnes-3.0-flash, 非流式) ==")
    body = {
        "model": "agnes-3.0-flash",
        "messages": [{"role": "user", "content": "用一句话介绍北京。"}],
        "stream": False,
    }
    r = httpx.post(f"{BASE}/v1/chat/completions", json=body, timeout=60)
    print("status:", r.status_code, "| X-Upstream:", r.headers.get("X-Upstream"))
    data = r.json()
    # 确认 model 名原样返回（未被改写）
    print("returned model:", data.get("model"))
    print("content:", (data.get("choices", [{}])[0].get("message", {}).get("content", ""))[:80])
    return r.status_code == 200 and data.get("model") == "agnes-3.0-flash"


def chat_stream():
    print("\n== /v1/chat/completions (agnes-3.0-flash, 流式) ==")
    body = {
        "model": "agnes-3.0-flash",
        "messages": [{"role": "user", "content": "数到 3。"}],
        "stream": True,
    }
    chunks = 0
    with httpx.stream("POST", f"{BASE}/v1/chat/completions", json=body, timeout=60) as r:
        print("status:", r.status_code, "| content-type:", r.headers.get("content-type"))
        for line in r.iter_lines():
            if line.startswith("data:"):
                chunks += 1
    print("streamed chunks:", chunks)
    return r.status_code == 200 and chunks > 0


def messages_anthropic():
    print("\n== /v1/messages (anthropic 协议, agnes-2.5-pro) ==")
    body = {
        "model": "agnes-2.5-pro",
        "max_tokens": 256,
        "messages": [{"role": "user", "content": "用一句话解释什么是相对论。"}],
    }
    r = httpx.post(
        f"{BASE}/v1/messages",
        json=body,
        headers={"anthropic-version": "2023-06-01"},
        timeout=90,
    )
    print("status:", r.status_code, "| X-Upstream:", r.headers.get("X-Upstream"))
    data = r.json()
    print("returned model:", data.get("model"))
    # anthropic 协议：reasoning 可能在 content 块的 thinking 类型里
    types = [b.get("type") for b in data.get("content", [])]
    print("content block types:", types)
    text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
    print("text:", text[:80])
    return r.status_code == 200 and data.get("model") == "agnes-2.5-pro"


def responses_reasoning():
    print("\n== /v1/responses (OpenAI Responses, agnes-2.5-pro, 看 reasoning) ==")
    body = {
        "model": "agnes-2.5-pro",
        "input": "1+1 等于几？只要答案。",
        "reasoning": {"effort": "low"},
    }
    r = httpx.post(f"{BASE}/v1/responses", json=body, timeout=90)
    print("status:", r.status_code, "| X-Upstream:", r.headers.get("X-Upstream"))
    data = r.json()
    print("returned model:", data.get("model"))
    # 检查 reasoning/推理字段是否原样透传
    has_reasoning = any(
        k for k in ("reasoning", "reasoning_details", "reasoning_summary")
        if k in data
    )
    print("has reasoning fields:", has_reasoning, "| keys:", sorted(data.keys())[:12])
    return r.status_code == 200 and data.get("model") == "agnes-2.5-pro"


if __name__ == "__main__":
    results = {}
    results["status"] = status()
    results["chat_nonstream"] = chat_nonstream()
    results["chat_stream"] = chat_stream()
    results["messages_anthropic"] = messages_anthropic()
    results["responses_reasoning"] = responses_reasoning()
    print("\n==== 验证汇总 ====")
    for k, v in results.items():
        print(f"  {k:20s}: {'OK' if v else 'FAIL'}")
