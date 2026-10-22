#!/usr/bin/env python3
"""v2.14 夹具第24相：write-ahead intent spool——纯函数层全行为钉。

她 2026-10-23 拍板的技术实现（设计正本 ~/plans/tideline-spool-design-v0.md），
发布梯子第一 rung：本夹具全绿 = spool.py 可独立 commit，第二 rung（server.py
集成，env 门缺省关）另开相。

钉子（行为合同）：
  24a enqueue 秒回小票：回执含 ticket_id/in_flight/state=pending，票在表里
  24b drain 顺序搬票：apply_fn 收到 intent 信封（tool+args 解析正确）
  24c 一票一事务：apply_fn 崩溃 → 域插入一并回滚 → 票仍 pending → 重放成功
      【崩溃安全主钉——她的原话「小票落盘就算数，崩了重放即可」】
  24d 毒票隔离：apply_fn 连败 3 次 → state=failed，不堵后面的队
  24e 老库零迁移：无表老库上 enqueue 幂等补表；pending_count/footer 缺表返 0/空
  24f presence 合并：pending_footer 报在途清单（id/tool/gesture 截断），空队=空串
  24g 审计三件套：text_hash 稳定（同 intent 同 hash）、created_by 落账、
      同内容双写合法（hash 不做去重键，日常真重复是允许的）

数据语义注意（夹具数据注释义务）：
  write_spool.state ∈ {pending, applied, failed}；attempts 只在失败路累计。
  票 #1/#2/#3 按 id 排序即入队序——drain 按 id 升序搬，先递先进。
  24d 毒票是 id 最小的票：连续失败隔离后，后面的票才能被搬到（隔离=让路）。

变异测试的牙：拆 24c 的事务保护（apply_fn 崩后不回滚、只翻票不回滚域插入）
→ 24c 必红；拆 24d 的 max_attempts 判定 → 24d 必红。

跑法: /home/ubuntu/.hermes/hermes-agent/venv/bin/python tests/fixture_phase24_spool.py
"""
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(REPO))

spec = importlib.util.spec_from_file_location("spool", REPO / "spool.py")
spool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(spool)

PASSES, FAILS = [], []


def check(name, cond, detail=""):
    if cond:
        PASSES.append(name)
        print(f"  ✅ {name}")
    else:
        FAILS.append(name)
        print(f"  ❌ {name}  {detail}")


def fresh_db():
    path = tempfile.mktemp(suffix=".db")
    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    return c


# ── 域侧替身：把 intent 落进一张假 narratives 表（机械替身，不 import server）──
def make_apply(table="narratives"):
    def apply_fn(c, intent):
        if intent.get("args", {}).get("poison"):   # 毒标记在 args 里（首跑勘误：别去 intent 顶层找）
            raise RuntimeError(f"apply failed for poison ticket")
        args = intent["args"]
        c.execute(
            f"INSERT INTO {table}(gesture, tags, created_at) VALUES(?,?,?)",
            (args.get("gesture", ""), json.dumps(args.get("tags", [])),
             "2026-10-23 00:00:00"),
        )
    return apply_fn


def main():
    print("═══ Phase 24: write-ahead intent spool（纯函数层）═══")

    # ── 24a enqueue 秒回小票 ──────────────────────────────
    print("── 24a enqueue 秒回小票 ──")
    c = fresh_db()
    c.execute("CREATE TABLE narratives(id INTEGER PRIMARY KEY, gesture TEXT, tags TEXT, created_at TEXT)")
    r1 = spool.enqueue(c, "memory_write", {"gesture": "第一票", "tags": ["a"]}, created_by="洄")
    r2 = spool.enqueue(c, "memory_write", {"gesture": "第二票", "tags": ["b"]}, created_by="照照")
    check("24a-1 回执含 ticket_id 且递增", r1["ticket_id"] == 1 and r2["ticket_id"] == 2,
          f"{r1} {r2}")
    check("24a-2 in_flight 反映队列深度", r1["in_flight"] == 1 and r2["in_flight"] == 2,
          f"{r1['in_flight']}→{r2['in_flight']}")
    check("24a-3 票在表里 state=pending",
          c.execute("SELECT COUNT(*) n FROM write_spool WHERE state='pending'").fetchone()["n"] == 2)

    # ── 24b drain 顺序搬票 ────────────────────────────────
    print("── 24b drain 顺序搬票 ──")
    got = []

    def spy_apply(c_, intent):
        got.append(intent)
        make_apply()(c_, intent)

    out = spool.drain(c, spy_apply)
    check("24b-1 两票全 applied", out["applied"] == [1, 2], str(out))
    check("24b-2 apply_fn 收到完整信封（tool+args）",
          got[0]["tool"] == "memory_write" and got[0]["args"]["gesture"] == "第一票",
          str(got[0])[:80])
    n_rows = c.execute("SELECT COUNT(*) n FROM narratives").fetchone()["n"]
    check("24b-3 域表真进了两行", n_rows == 2, f"n={n_rows}")

    # ── 24c 一票一事务（崩溃安全主钉）────────────────────
    print("── 24c 一票一事务：崩溃→回滚→重放 ──")
    c2 = fresh_db()
    c2.execute("CREATE TABLE narratives(id INTEGER PRIMARY KEY, gesture TEXT, tags TEXT, created_at TEXT)")
    spool.enqueue(c2, "memory_write", {"gesture": "崩溃票", "tags": ["x"]}, created_by="洄")
    crashes = {"n": 0}

    def crash_once(c_, intent):
        crashes["n"] += 1
        make_apply()(c_, intent)   # 先把域插入做完再崩——模拟「写到一半崩」（首跑变异哑钉勘误：插入前崩=无物可回滚，牙咬不到轴）
        if crashes["n"] == 1:
            raise RuntimeError("simulated crash mid-apply")

    out1 = spool.drain(c2, crash_once)
    row = c2.execute("SELECT state, attempts FROM write_spool WHERE id=1").fetchone()
    check("24c-1 崩溃后票仍 pending", row["state"] == "pending", str(dict(row)))
    check("24c-2 崩溃不落域行（回滚干净）",
          c2.execute("SELECT COUNT(*) n FROM narratives").fetchone()["n"] == 0)
    check("24c-3 attempts 记账", row["attempts"] == 1, str(dict(row)))
    out2 = spool.drain(c2, crash_once)
    check("24c-4 重放成功", out2["applied"] == [1], str(out2))
    check("24c-5 重放后域行恰好一行（无重复）",
          c2.execute("SELECT COUNT(*) n FROM narratives").fetchone()["n"] == 1)
    check("24c-6 重放后票翻 applied",
          c2.execute("SELECT state FROM write_spool WHERE id=1").fetchone()["state"] == "applied")

    # ── 24d 毒票隔离 ──────────────────────────────────────
    print("── 24d 毒票隔离：连败 3 次进隔离区 ──")
    c3 = fresh_db()
    c3.execute("CREATE TABLE narratives(id INTEGER PRIMARY KEY, gesture TEXT, tags TEXT, created_at TEXT)")
    spool.enqueue(c3, "memory_write", {"gesture": "毒票", "poison": True}, created_by="洄")
    spool.enqueue(c3, "memory_write", {"gesture": "好票A", "tags": ["g"]}, created_by="照照")
    outA = spool.drain(c3, make_apply())
    outB = spool.drain(c3, make_apply())
    outC = spool.drain(c3, make_apply())
    poison = c3.execute("SELECT state, attempts FROM write_spool WHERE id=1").fetchone()
    good = c3.execute("SELECT state FROM write_spool WHERE id=2").fetchone()
    check("24d-1 毒票三连败后 failed", poison["state"] == "failed", str(dict(poison)))
    check("24d-2 好票被让路 applied", good["state"] == "applied", str(dict(good)))
    check("24d-3 域表只有好票的行",
          c3.execute("SELECT gesture FROM narratives").fetchall()[0]["gesture"] == "好票A")

    # ── 24e 老库零迁移 ────────────────────────────────────
    print("── 24e 老库零迁移 ──")
    c4 = fresh_db()
    c4.execute("CREATE TABLE narratives(id INTEGER PRIMARY KEY, gesture TEXT, tags TEXT, created_at TEXT)")
    check("24e-1 老库缺表 pending_count=0 不炸", spool.pending_count(c4) == 0)
    check("24e-2 老库缺表 footer=空串", spool.pending_footer(c4) == "")
    spool.enqueue(c4, "memory_write", {"gesture": "补票"}, created_by="洄")
    check("24e-3 enqueue 幂等补表", spool.pending_count(c4) == 1)
    spool.drain(c4, make_apply())
    check("24e-4 补表后 drain 正常", spool.pending_count(c4) == 0)

    # ── 24f presence 合并 ─────────────────────────────────
    print("── 24f presence 合并（读时合并 v0）──")
    c5 = fresh_db()
    c5.execute("CREATE TABLE narratives(id INTEGER PRIMARY KEY, gesture TEXT, tags TEXT, created_at TEXT)")
    check("24f-1 空队 footer=空串", spool.pending_footer(c5) == "")
    spool.enqueue(c5, "memory_write", {"gesture": "在途票一"}, created_by="鸣鸣")
    spool.enqueue(c5, "memory_write", {"gesture": "在途票二"}, created_by="鸣鸣")
    foot = spool.pending_footer(c5)
    check("24f-2 footer 报在途数", "在途 2 条" in foot, foot[:60])
    check("24f-3 footer 带票 id+tool+gesture 截断",
          "#1 [memory_write] 在途票一" in foot and "#2 [memory_write] 在途票二" in foot, foot[:100])
    check("24f-4 footer 带 created_by", "(by 鸣鸣)" in foot, foot[-60:])

    # ── 24g 审计三件套 ────────────────────────────────────
    print("── 24g 审计三件套 ──")
    h1 = spool._hash_intent("memory_write", {"gesture": "同", "tags": ["t"]})
    h2 = spool._hash_intent("memory_write", {"gesture": "同", "tags": ["t"]})
    h3 = spool._hash_intent("memory_write", {"gesture": "不同", "tags": ["t"]})
    check("24g-1 text_hash 稳定（同 intent 同 hash）", h1 == h2)
    check("24g-2 内容变 hash 变", h1 != h3)
    c6 = fresh_db()
    spool.enqueue(c6, "memory_write", {"gesture": "审计票"}, created_by="琴师")
    row6 = c6.execute("SELECT text_hash, created_by FROM write_spool WHERE id=1").fetchone()
    check("24g-3 created_by 落账", row6["created_by"] == "琴师", str(dict(row6)))
    spool.enqueue(c6, "memory_write", {"gesture": "审计票"}, created_by="琴师")
    rows = c6.execute("SELECT COUNT(*) n FROM write_spool").fetchone()["n"]
    check("24g-4 同内容双写合法（hash 是审计指纹非去重键）", rows == 2)

    total = len(PASSES) + len(FAILS)
    print("=" * 50)
    print(f"{len(PASSES)}/{total} passed" + (" — FAILED" if FAILS else ""))
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
