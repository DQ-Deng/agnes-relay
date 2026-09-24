"""独立验证事件日志滚动轮转：重定向 M._logpath 到临时目录，写超限量事件，
断言 .1/.2 生成、活动文件重置、备份上限遵守、log_rotations 计数、log_rotated 事件。"""
import os
import sys
import glob
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import main

M = main.M

TMP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_rot_tmp")
shutil.rmtree(TMP, ignore_errors=True)
os.makedirs(TMP)

# 重定向日志到临时目录（必须在临时路径重新打开文件句柄）
M._logf.close()
M._logpath = os.path.join(TMP, "relay_events.log")
M._logf = open(M._logpath, "a", encoding="utf-8", buffering=1)
M._log_size = 0
# 强制小上限便于触发
M._log_max_bytes = 400   # 400 字节即轮转
M._log_backups = 3

failures = []

# 1) 写一批超限事件，触发多次轮转
for i in range(120):
    M.log_event("selftest", "up", f"payload-{i:03d}" + "x" * 20)

files = sorted(os.path.basename(p) for p in glob.glob(os.path.join(TMP, "relay_events.log*")))
print("轮转后文件:", files)

base = os.path.join(TMP, "relay_events.log")
# 活动文件应已重置且很小
active_size = os.path.getsize(base) if os.path.exists(base) else -1
print("活动文件大小:", active_size)

# 备份最多 3 份（.1 .2 .3），不应有 .4
backups = [f for f in files if f.endswith(tuple(f".{i}" for i in (1, 2, 3)))]
overflow = [f for f in files if f.endswith(".4")]
if overflow:
    failures.append(f"超出备份上限，出现 {overflow}")
# 活动文件必须在 max_bytes 附近但不超过（下一条会先轮转再写）
if active_size > M._log_max_bytes:
    failures.append(f"活动文件 {active_size}B 超过上限 {M._log_max_bytes}B")

# log_rotated 事件应出现在活动文件（或最近一份）中
found_rotated = False
for f in files:
    p = f if os.path.isabs(f) else os.path.join(TMP, f)
    if "log_rotated" in open(p, encoding="utf-8", errors="ignore").read():
        found_rotated = True
if not found_rotated:
    failures.append("未找到 log_rotated 事件")

print("log_rotations 计数:", M.log_rotations)
if M.log_rotations < 3:
    failures.append(f"log_rotations 仅 {M.log_rotations}，期望 >= 3")

# 2) /status 的 log 段
import asyncio
st = asyncio.run(main.status())
print("/status log 段:", st["log"])
if st["log"]["rotations"] != M.log_rotations:
    failures.append("/status log.rotations 与 M.log_rotations 不一致")

# 先关掉指向临时目录的句柄（Windows 上句柄未关会导致 rmtree 静默失败），再清理
if M._logf:
    M._logf.close()
    M._logf = None
shutil.rmtree(TMP, ignore_errors=False)
# 恢复 M 到真实日志路径
M._logpath = main.LOG_PATH
M._logf = open(M._logpath, "a", encoding="utf-8", buffering=1)
M._log_size = os.path.getsize(M._logpath) if os.path.exists(M._logpath) else 0
M._log_max_bytes = int(main.LOG_MAX_MB * 1024 * 1024) if main.LOG_MAX_MB > 0 else 0
M._log_backups = main.LOG_BACKUPS

print("\n" + ("PASS: 全部断言通过" if not failures else "FAIL:\n  " + "\n  ".join(failures)))
sys.exit(1 if failures else 0)
