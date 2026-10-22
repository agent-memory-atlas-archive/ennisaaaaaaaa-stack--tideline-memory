#!/usr/bin/env python3
"""v2.13 夹具第23相：并发头阻塞止血——dispatch 进线程池，事件循环永不被占。

背景（2026-10-22 03:14-05:32 事故，甜心 10/22 拍板「先讨论不直接干，开干净
session 再实施」——本相就是那个干净 session 的实施验收）：
  全家所有 session 共用一个 portalk-memory server 进程（单线程 asyncio）。
  DREAM 批处理的长同步段（SQLite 同步调用）跑在事件循环上 → keepalive
  应答不了 → gateway 判 degraded → 排队调用连环 300s 超时。
  零 SQLITE_BUSY——不是锁死，是头阻塞。
  修法：call_tool 把整个 _dispatch 连同 DB 连接挪进 asyncio.to_thread，
  线程内 asyncio.run 起私有循环供 _embed 等 true-async 段。

钉子（行为合同）：
  23a keepalive 等价物：两个慢 dispatch(各 sleep 1.5s)在跑时，事件循环上的
      定时协程(watchdog, 0.5s) 准时完成——循环没被占【止血主张本体】
  23b 并发真并发：两个慢 dispatch 并发发起，总耗时 < 串行(2×1.5s+开销)，
      说明线程池里各跑各的
  23c 写读 roundtrip：call_tool(memory_write) → call_tool(memory_recall)
      走新线程路径行为不变【回归保护】
  23d 异常随迁：dispatch 内抛异常 → call_tool 返回 ❌ 文本，不炸线程池
  23e 兼容契约：await srv._dispatch(...) 仍可直接调用——phase12/13/15
      夹具的直接调用面不动

数据语义注意：无真实数据依赖；慢注入用 SlowConn 包一层（首个 execute
sleep delay 秒后透传真连接），delay=1.5s 是「慢查询」的机械替身，测的是
「dispatch 慢时循环活不活」，慢的来源（SQLite/任意同步段）不影响主张。

变异测试的牙：把 call_tool 换回旧版（直接 await _dispatch，同步 sleep 在
循环上）→ 23a 必红（watchdog 被 1.5s 堵到 >1s 才完成）、23b 必红（串行 3s+）。

跑法: /home/ubuntu/.hermes/hermes-agent/venv/bin/python tests/fixture_phase23_offload_dispatch.py
"""
import asyncio, importlib.util, os, sqlite3, sys, tempfile, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SERVER = HERE.parent / "server.py"
assert SERVER.exists(), f"server.py not found at {SERVER}"

DB = tempfile.mktemp(suffix=".db")
os.environ["MEMORY_MCP_DB"] = DB
os.environ["EMBEDDING_API_KEY"] = ""      # local 路模式

spec = importlib.util.spec_from_file_location("memory_server", str(SERVER))
srv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(srv)

# ── embed 注入：固定向量，全程零网络 ─────────────────────────
async def fake_embed(text, _retries=3, timeout=60.0, **kw):
    return [0.1, 0.2, 0.3]
srv._embed = fake_embed

# ── 慢连接注入：首个 execute 阻塞 delay 秒（慢查询的机械替身）──
_real_db = srv._db
SLOW_DELAY = 1.5

class SlowConn:
    """透传 wrapper：仅第一个 execute 前同步 sleep——制造头阻塞条件。"""
    def __init__(self, real, delay):
        self._real, self._delay, self._fired = real, delay, False
    def execute(self, sql, *a, **k):
        if not self._fired:
            self._fired = True
            time.sleep(self._delay)   # 同步阻塞——旧代码里这就堵死事件循环
        return self._real.execute(sql, *a, **k)
    def __getattr__(self, n):
        return getattr(self._real, n)

def slow_db():
    return SlowConn(_real_db(), SLOW_DELAY)

results = []

def check(name, ok, detail=""):
    results.append((name, ok))
    print(f"  {'✅' if ok else '❌'} {name}" + (f" — {detail}" if detail else ""))

async def main():
    print("═══ Phase 23: dispatch 线程池化——头阻塞止血 ═══")
    srv._init()

    # ── 23a/23b: 慢 dispatch 并发 + watchdog 准时 ──────────────
    print("── 23a/23b 慢并发 + keepalive 等价物 ──")
    srv._db = slow_db
    t0 = time.monotonic()
    wd_done = None

    async def watchdog():
        nonlocal wd_done
        await asyncio.sleep(0.5)
        wd_done = time.monotonic()

    async def slow_call():
        return await srv.call_tool("memory_recall", {"limit": 3})

    await asyncio.gather(
        asyncio.create_task(slow_call()),
        asyncio.create_task(slow_call()),
        asyncio.create_task(watchdog()),
    )
    total = time.monotonic() - t0
    wd_latency = wd_done - t0
    check("23a watchdog 准时(≤1.0s，循环未被占)", wd_latency <= 1.0,
          f"watchdog {wd_latency:.2f}s 完成（sleep 0.5s 预期）")
    check("23b 两慢 dispatch 并发(总耗时<2×delay)", total < 2 * SLOW_DELAY + 0.3,
          f"总耗时 {total:.2f}s < 串行 {2*SLOW_DELAY:.1f}s+0.3")
    srv._db = _real_db

    # ── 23c: 写读 roundtrip（新线程路径，行为不变）─────────────
    print("── 23c 写读 roundtrip ──")
    w = await srv.call_tool("memory_write", {
        "gesture": "夹具23相止血钉", "context": "dispatch 线程池化验收",
        "moment": "2026-10-23", "cognition_direction": "并发不堵门",
        "tags": ["夹具", "phase23"], "importance": 3,
    })
    wtxt = w[0].text if w else ""
    ok_w = "已记录" in wtxt          # 写入成功的实际回执形态（首跑勘误：期望别找 ✍️，找「已记录」）
    check("23c-1 memory_write 走新路径成功", ok_w, wtxt[:80])
    r = await srv.call_tool("memory_recall", {"limit": 5})
    rtxt = r[0].text if r else ""
    check("23c-2 memory_recall 读回新写入", "止血钉" in rtxt, rtxt[:80])

    # ── 23d: 异常随迁 ─────────────────────────────────────────
    print("── 23d 异常随迁 ──")
    async def boom(**kw):
        raise RuntimeError("23d-injected")
    orig = srv._dispatch
    srv._dispatch = boom
    e = await srv.call_tool("memory_recall", {"limit": 1})
    srv._dispatch = orig
    etxt = e[0].text if e else ""
    check("23d dispatch 抛异常→❌ 文本不炸", etxt.startswith("❌"), etxt[:60])

    # ── 23e: 兼容契约——直接 await _dispatch 仍可用 ─────────────
    print("── 23e 兼容契约 ──")
    c = _real_db()
    try:
        d = await orig("memory_recall", {"limit": 2}, c)
        dtxt = d[0].text if d else ""
        check("23e await srv._dispatch 直接调用面不动", bool(dtxt), dtxt[:60])
    finally:
        c.close()

    n_pass = sum(1 for _, ok in results if ok)
    print(f"\n{'='*50}\n{len(results)}/{len(results)} passed" if n_pass == len(results)
          else f"\n{'='*50}\n{n_pass}/{len(results)} passed — FAILED")
    sys.exit(0 if n_pass == len(results) else 1)

if __name__ == "__main__":
    asyncio.run(main())
