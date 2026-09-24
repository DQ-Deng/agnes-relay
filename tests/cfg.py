"""测试脚本共享配置加载：从仓库根目录 config.toml 读取 key 与上游地址。

用法（在各测试脚本顶部）：
    from cfg import load
    UP, K1, K2 = load()

这样脚本本身不再携带任何密钥；密钥只存在于已被 .gitignore 排除的 config.toml 中。
注意：以 `python tests/xxx.py` 方式运行时，sys.path[0] 是 tests/ 目录，故 `import cfg` 直接可用。
"""
import os
import sys
import tomllib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load() -> tuple[str, str, str]:
    """读取 config.toml[upstream]，返回 (base_url, key_1, key_2)。

    config.toml 缺失、key 未填或仍是占位符时，打印清晰提示并退出（退出码 1）。
    """
    path = os.path.join(_ROOT, "config.toml")
    try:
        with open(path, "rb") as f:
            up = tomllib.load(f).get("upstream", {})
    except FileNotFoundError:
        sys.exit(
            f"[cfg] 找不到 {path}\n"
            f"      请先: copy config.example.toml config.toml 并填入真实 key"
        )
    except Exception as e:
        sys.exit(f"[cfg] 解析 config.toml 失败: {e}")

    base_url = up.get("base_url") or "https://api.agnes-ai.cn"
    k1, k2 = up.get("key_1"), up.get("key_2")

    problems = []
    for i, k in ((1, k1), (2, k2)):
        if not k:
            problems.append(f"key_{i} 为空")
        elif "在这里填" in k:
            problems.append(f"key_{i} 仍是占位符，未填真实 key")
    if problems:
        sys.exit(
            f"[cfg] config.toml [upstream] 配置不完整: {'; '.join(problems)}\n"
            f"      请填入真实 key 后再运行本脚本（中继相关请求不需要 key，直连对比需要）。"
        )
    return base_url, k1, k2
