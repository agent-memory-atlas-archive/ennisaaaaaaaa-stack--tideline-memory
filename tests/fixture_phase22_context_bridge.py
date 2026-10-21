#!/usr/bin/env python3
"""phase22: T2b context bridge — fix 'oldest-5' bug, pin latest-N behavior.

Bug: rows came back DESC (new→old), appended in that order, then [-max_chunk:]
took the list tail = the OLDEST 5 entries of the chosen session.
Docstring says "Return the latest N chunks" — implementation contradicted it.

Fix: take the newest max_chunk entries (list head after DESC append),
then reverse to chronological order for display.

数据语义注意（首轮夹具的伤）：entries 按 (sess, marker, mins_ago) 生成，
mins_ago 越小越新。MARK1 若 mins_ago=10 则 MARK1 是最新——别按编号直觉走。

Self-contained: builds its own :memory:-style temp DB, imports provider by path.
Run: /home/ubuntu/.hermes/hermes-agent/venv/bin/python tests/fixture_phase22_context_bridge.py
"""
import importlib.util
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

spec = importlib.util.spec_from_file_location(
    "tideline_provider", os.path.join(REPO, "plugins", "tideline_provider.py"))
tp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tp)

PASS = 0
TOTAL = 0

NARRATIVES_DDL = ("CREATE TABLE IF NOT EXISTS narratives "
                  "(id INTEGER PRIMARY KEY, weight INTEGER, source_links TEXT)")
CTX_DDL = ("CREATE TABLE context (id INTEGER PRIMARY KEY AUTOINCREMENT,"
           " content TEXT, meta TEXT, created_at TEXT)")


def check(name, cond, detail=""):
    global PASS, TOTAL
    TOTAL += 1
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if cond:
        PASS += 1


def make_db(entries):
    """entries: list of (session, marker, minutes_ago). mins_ago 小 = 新。"""
    db = os.path.join(tempfile.mkdtemp(), "t.db")
    con = sqlite3.connect(db)
    con.execute(CTX_DDL)
    con.execute(NARRATIVES_DDL)
    now = datetime.utcnow()
    for sess, marker, mins_ago in entries:
        ts = (now - timedelta(minutes=mins_ago)).strftime("%Y-%m-%d %H:%M:%S")
        con.execute("INSERT INTO context (content, meta, created_at) VALUES (?,?,?)",
                    (marker, f'{{"session": "{sess}"}}', ts))
    con.commit()
    con.close()
    return db


def run_bridge(db_path):
    prov = tp.TidelineMemoryProvider.__new__(tp.TidelineMemoryProvider)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        return prov._context_bridge(con)
    finally:
        con.close()


def markers_of(text):
    # 内容形如 "MARK8|"（管道在 marker 之后），按行取首段
    return [ln.split("|")[0].strip() for ln in text.splitlines() if "|" in ln]


print("== phase22: T2b context bridge latest-N fix ==")

# ── E1: 单会话 8 条，取最新 5 条，时间正序展示 ──
# 生成时 mins_ago = 10*i：i=1(MARK1,10min前)=最新 … i=8(MARK8,80min前)=最旧
entries = [("s1", f"MARK{i}|", 10 * i) for i in range(1, 9)]
db = make_db(entries)
out = run_bridge(db)
ms = markers_of(out)
check("E1 newest-5 chosen", sorted(ms) == ["MARK1", "MARK2", "MARK3", "MARK4", "MARK5"], f"got {ms}")
check("E1a chronological ascending (MARK5→MARK1, 旧→新)", ms == ["MARK5", "MARK4", "MARK3", "MARK2", "MARK1"], f"got {ms}")

# ── E2: 两会话并存，最新会话胜 ──
entries = ([("old_sess", f"OLD{i}|", 200 + 10 * i) for i in range(1, 5)]
           + [("new_sess", f"NEW{i}|", 10 * i) for i in range(1, 5)])
db = make_db(entries)
out = run_bridge(db)
ms = markers_of(out)
check("E2 most-recent session selected", all(m.startswith("NEW") for m in ms) and len(ms) == 4, f"got {ms}")

# ── E3: meta 为 NULL 的行归 _legacy 桶，不崩 ──
db = os.path.join(tempfile.mkdtemp(), "t3.db")
con = sqlite3.connect(db)
con.execute(CTX_DDL)
con.execute(NARRATIVES_DDL)
now = datetime.utcnow()
for i in range(1, 4):
    ts = (now - timedelta(minutes=5 * i)).strftime("%Y-%m-%d %H:%M:%S")
    con.execute("INSERT INTO context (content, meta, created_at) VALUES (?,?,?)",
                (f"LEG{i}|", None, ts))
con.commit(); con.close()
out = run_bridge(db)
ms = markers_of(out)
check("E3 NULL meta → _legacy bucket, no crash", sorted(ms) == ["LEG1", "LEG2", "LEG3"], f"got {ms}")

# ── E4: 72h 之外无数据 → 空串 ──
db = os.path.join(tempfile.mkdtemp(), "t4.db")
con = sqlite3.connect(db)
con.execute(CTX_DDL)
con.execute(NARRATIVES_DDL)
ts = (datetime.utcnow() - timedelta(hours=100)).strftime("%Y-%m-%d %H:%M:%S")
con.execute("INSERT INTO context (content, meta, created_at) VALUES (?,?,?)",
            ("STALE|", '{"session": "s"}', ts))
con.commit(); con.close()
out = run_bridge(db)
check("E4 beyond-72h → empty string", out == "", f"got {out[:60]!r}")

# ── E5: 71h 远会话 vs 1min 近会话，近者胜 ──
db = os.path.join(tempfile.mkdtemp(), "t5.db")
con = sqlite3.connect(db)
con.execute(CTX_DDL)
con.execute(NARRATIVES_DDL)
ts71 = (datetime.utcnow() - timedelta(hours=71)).strftime("%Y-%m-%d %H:%M:%S")
ts_new = (datetime.utcnow() - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
con.execute("INSERT INTO context (content, meta, created_at) VALUES (?,?,?)", ("71H|", '{"session": "far"}', ts71))
con.execute("INSERT INTO context (content, meta, created_at) VALUES (?,?,?)", ("RECENT|", '{"session": "near"}', ts_new))
con.commit(); con.close()
out = run_bridge(db)
ms = markers_of(out)
check("E5 recent beats 71h-far session", ms == ["RECENT"], f"got {ms}")

# ── E6: 回归钉——旧 bug（尾切最旧5条）下 MARK6/7/8 会入选；修复后必须缺席 ──
db = make_db(entries=[("s1", f"MARK{i}|", 10 * i) for i in range(1, 9)])
out = run_bridge(db)
ms = markers_of(out)
check("E6 regression pin: MARK6-8 (oldest) absent", not any(m in ms for m in ["MARK6", "MARK7", "MARK8"]), f"got {ms}")

print(f"\n{PASS}/{TOTAL} passed")
sys.exit(0 if PASS == TOTAL else 1)
