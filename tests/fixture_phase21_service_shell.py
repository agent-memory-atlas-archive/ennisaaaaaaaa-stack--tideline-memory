#!/usr/bin/env python3
"""v0.x 服务壳夹具（Tideline 服务化 A 块）：FastAPI 三口的机械验收。

spec: ~/plans/tideline-服务化-spec-v0.md（洄 2026-10-14，svps 正本；照照 D_A 判据）

验收点：
  S1 三口全活：/health 200、POST /event 200 ok+id 回显、GET /salience 排行
  S2 幂等：同 event_id 重发→duplicate；无 event_id 两笔各得各的 id
  S3 校验：缺 ts 422、缺 organ_id 422、非 JSON 422——报错都带 "need" 字段名
  S4 乱序迟到：晚到的旧事件插入后，新事件的衰减账不被污染（各自按 ts 衰减）
  S5 衰减单调：同 hint，老事件贡献 < 新事件贡献；窗口外事件不入账
  S6 显著度与 verdict：匹配事件攒够分→verdict=wake；不匹配的 thread=quiet；
     单 thread 查询 404 时报错带 "need"
  S7 降级：记忆库缺失→/salience 返回空排行不炸、/health memory_db_ok=false；
     POST /event 照常落台账（服务壳活命不依赖记忆库）
  S8 /events 增量：since_id 翻页、event_json 原始信封可回放
  S9 契约：docstring 声明「不换存储/sidecar/影子模式」三边界 + body 权威 + ts_coerced 出声

跑法: python3 tests/fixture_phase21_service_shell.py   （需 fastapi+httpx，tempfile 库零残留）
"""
import importlib.util, json, sqlite3, sys, tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
MODULE = HERE.parent / "service_shell.py"
assert MODULE.exists(), f"service_shell.py not found at {MODULE}"

from fastapi.testclient import TestClient  # noqa: E402

FAILS = []
PASSES = []


def check(name, cond, detail=""):
    if cond:
        PASSES.append(name)
        print(f"  ✅ {name}")
    else:
        FAILS.append(name)
        print(f"  ❌ {name}  {detail}")


def load_shell(memory_db: str, events_db: str):
    """按库路径各加载一份独立模块实例（env 在 import 时固化）。"""
    import os
    old = {k: os.environ.get(k) for k in ("MEMORY_MCP_DB", "TIDELINE_SERVICE_DB")}
    os.environ["MEMORY_MCP_DB"] = memory_db
    os.environ["TIDELINE_SERVICE_DB"] = events_db
    spec = importlib.util.spec_from_file_location(
        f"service_shell_{abs(hash(events_db)) & 0xffffff:x}", MODULE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    return mod


def seed_memory(db: str):
    """最小记忆库：threads 两根 open + 一根 done。"""
    conn = sqlite3.connect(db)
    conn.executescript("""
CREATE TABLE threads(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  content TEXT NOT NULL, importance INTEGER, emotional INTEGER,
  recurrence INTEGER, unresolved INTEGER, weight REAL,
  status TEXT DEFAULT 'open', created_at TEXT, explored_at TEXT);
""")
    conn.execute("INSERT INTO threads(content,importance,unresolved,weight,status,created_at) "
                 "VALUES('琴师自组织与画笔方法论',4,4,2.0,'open','2026-10-01T00:00:00+00:00')")
    conn.execute("INSERT INTO threads(content,importance,unresolved,weight,status,created_at) "
                 "VALUES('服务化灰度窗口',3,3,1.5,'open','2026-10-14T00:00:00+00:00')")
    conn.execute("INSERT INTO threads(content,importance,unresolved,weight,status,created_at) "
                 "VALUES('已完结的旧线索',2,1,1.0,'done','2026-09-01T00:00:00+00:00')")
    conn.execute("CREATE TABLE profiles(id INTEGER PRIMARY KEY AUTOINCREMENT, entity TEXT, "
                 "ptype TEXT, content TEXT, updated_at TEXT)")
    conn.execute("INSERT INTO profiles(entity,ptype,content,updated_at) "
                 "VALUES('琴师','impression','画笔方法论的自组织样本','2026-10-10T00:00:00+00:00')")
    conn.commit()
    conn.close()


def ev(ts, organ="rss", hint=1.0, entities=(), summary="...", event_id=None, etype="digest"):
    d = {"ts": ts, "organ_id": organ, "modality": "world",
         "event_type": etype, "saliency_hint": hint,
         "payload_summary": summary, "entities": list(entities)}
    if event_id:
        d["event_id"] = event_id
    return d


def main():
    print("═══ Phase 21: 服务壳 A 块（FastAPI 三口） ═══")
    tmp = tempfile.mkdtemp(prefix="phase21-")
    mem_db = str(Path(tmp) / "mem.db")
    ev_db = str(Path(tmp) / "events.db")
    seed_memory(mem_db)
    mod = load_shell(mem_db, ev_db)
    client = TestClient(mod.app)

    now = datetime.now(timezone.utc)

    # ── S1 三口全活 ──
    print("── S1 三口全活 ──")
    h = client.get("/health")
    check("S1a /health 200", h.status_code == 200, h.text)
    hj = h.json()
    check("S1b health 带 events_total/memory_db_ok/旋钮", 
          "events_total" in hj and hj.get("memory_db_ok") is True and "knobs" in hj)
    r1 = client.post("/event", json=ev(now.isoformat(), entities=["琴师"], summary="画笔复盘"))
    check("S1c POST /event 200 ok + id 回显",
          r1.status_code == 200 and r1.json().get("status") == "ok" and isinstance(r1.json().get("id"), int),
          r1.text)
    s = client.get("/salience")
    check("S1d GET /salience 排行 200 且 open threads 两根",
          s.status_code == 200 and len(s.json().get("threads", [])) == 2, s.text)
    check("S1e done 线索不入排行",
          all("旧线索" not in t["thread"]["content"] for t in s.json()["threads"]))

    # ── S2 幂等 ──
    print("── S2 幂等 ──")
    payload = ev(now.isoformat(), entities=["琴师"], event_id="mk-001")
    a = client.post("/event", json=payload).json()
    b = client.post("/event", json=payload).json()
    check("S2a 同 event_id 首发ok 重发duplicate",
          a["status"] == "ok" and b["status"] == "duplicate", f"{a} / {b}")
    no_id1 = client.post("/event", json=ev(now.isoformat()))
    no_id2 = client.post("/event", json=ev(now.isoformat()))
    check("S2b 无 event_id 各得各的 id",
          no_id1.json()["id"] != no_id2.json()["id"] and no_id1.json()["status"] == "ok")

    # ── S3 校验 ──
    print("── S3 校验（报错报在人读得懂的地方） ──")
    e1 = client.post("/event", json={"organ_id": "rss"})
    check("S3a 缺 ts 422 带 need",
          e1.status_code == 422 and e1.json().get("need") == "ts", e1.text)
    e2 = client.post("/event", json={"ts": now.isoformat()})
    check("S3b 缺 organ_id 422 带 need",
          e2.status_code == 422 and e2.json().get("need") == "organ_id", e2.text)
    e3 = client.post("/event", content=b"not-json", headers={"Content-Type": "application/json"})
    check("S3c 非 JSON 422 带 need",
          e3.status_code == 422 and "need" in e3.json(), e3.text)

    # ── S4/S5 乱序迟到 + 衰减 ──
    print("── S4/S5 乱序迟到与衰减 ──")
    mod2_db = str(Path(tmp) / "ev2.db")
    mod2 = load_shell(mem_db, mod2_db)
    c2 = TestClient(mod2.app)
    t_new = now
    t_old = now - timedelta(hours=12)
    t_ancient = now - timedelta(hours=200)
    c2.post("/event", json=ev(t_new.isoformat(), entities=["琴师"], hint=1.0, summary="new"))
    # 迟到乱序：更老的事件晚到
    c2.post("/event", json=ev(t_old.isoformat(), entities=["琴师"], hint=1.0, summary="old"))
    c2.post("/event", json=ev(t_ancient.isoformat(), entities=["琴师"], hint=1.0, summary="ancient"))
    acc = c2.get("/salience").json()["threads"]
    matched = {m["payload_summary"]: m for t in acc for m in t["matched"]}
    check("S4a 迟到的老事件按自身 ts 衰减（decay=0.5@12h半衰期）",
          any(abs(m["decay"] - 0.5) < 1e-3 for m in matched.values() if m["payload_summary"] == "old"),
          json.dumps(list(matched.values()), ensure_ascii=False))
    check("S4b 新事件 decay=1.0 不被晚到的旧事件污染",
          any(m["decay"] == 1.0 for m in matched.values() if m["payload_summary"] == "new"))
    check("S5a 窗口外(200h)事件不入账",
          not any(m["payload_summary"] == "ancient" for m in matched.values()))
    n = next(t for t in acc if "琴师" in t["thread"]["content"])
    check("S5b score = Σ(hint×decay×增益) 与明细对账",
          abs(n["score"] - round(sum(m["contrib"] for m in n["matched"]), 4)) < 1e-6,
          f"score={n['score']} matched={[m['contrib'] for m in n['matched']]}")

    # ── S6 显著度与 verdict ──
    print("── S6 verdict ──")
    mod3_db = str(Path(tmp) / "ev3.db")
    mod3 = load_shell(mem_db, mod3_db)
    c3 = TestClient(mod3.app)
    for i in range(4):  # 4×1.0 全新事件 → 琴师 thread 应 wake（threshold=3.0）
        c3.post("/event", json=ev(now.isoformat(), entities=["琴师"], hint=1.0,
                                  summary=f"火{i}", event_id=f"w-{i}"))
    one = c3.get("/salience", params={"thread": 1}).json()
    check("S6a 攒够分 verdict=wake",
          one["verdict"] == "wake" and one["score"] >= 3.0, json.dumps(one)[:200])
    two = c3.get("/salience", params={"thread": 2}).json()
    check("S6b 不匹配的 thread quiet",
          two["verdict"] == "quiet" and two["score"] == 0.0)
    nf = c3.get("/salience", params={"thread": 999})
    check("S6c 未知 thread 404 带 need", nf.status_code == 404 and "need" in nf.json(), nf.text)

    # ── S7 降级：记忆库缺失 ──
    print("── S7 记忆库缺失降级 ──")
    mod4 = load_shell(str(Path(tmp) / "ghost.db"), str(Path(tmp) / "ev4.db")
                      )  # ghost.db 从未创建
    c4 = TestClient(mod4.app)
    s4 = c4.get("/salience")
    check("S7a 记忆库缺失 /salience 不炸、空排行、memory_db_ok=false",
          s4.status_code == 200 and s4.json()["threads"] == [] and s4.json()["memory_db_ok"] is False,
          s4.text)
    r4 = c4.post("/event", json=ev(now.isoformat(), event_id="no-mem-1"))
    check("S7b 记忆库缺失 POST 照常落台账",
          r4.status_code == 200 and r4.json()["status"] == "ok", r4.text)
    h4 = c4.get("/health").json()
    check("S7c health 如实报 memory_db_ok=false", h4["memory_db_ok"] is False)

    # ── S8 /events 增量翻页 + 原始信封回放 ──
    print("── S8 /events 增量 ──")
    page1 = c3.get("/events", params={"since_id": 0, "limit": 2}).json()
    page2 = c3.get("/events", params={"since_id": page1["next_since_id"], "limit": 50}).json()
    total = page1["count"] + page2["count"]
    check("S8a since_id 翻页无重无漏", total == 4 and page1["events"][0]["id"] < page2["events"][0]["id"],
          f"p1={page1['count']} p2={page2['count']}")
    conn = sqlite3.connect(mod3_db)
    raw = conn.execute("SELECT event_json FROM service_events WHERE event_id='w-0'").fetchone()
    conn.close()
    check("S8b event_json 原始信封可回放（含 entities）",
          raw and json.loads(raw[0]).get("entities") == ["琴师"])

    # ── S9 契约 ──
    print("── S9 契约 ──")
    doc = (HERE.parent / "service_shell.py").read_text(encoding="utf-8")
    check("S9a docstring 声明不换存储/sidecar/影子模式三边界",
          all(k in doc for k in ("不换存储", "sidecar", "影子")) and "真唤醒等灰度" in doc)
    check("S9b body 权威判别 + ts_coerced 出声", "ts_coerced" in doc and "duplicate" in doc)

    # ── S10 salience_context 真复用（不是 NULL 摆设） ──
    print("── S10 salience_context 快照 ──")
    conn = sqlite3.connect(ev_db)
    ctx_raw = conn.execute("SELECT salience_context FROM service_events "
                           "WHERE event_id='mk-001'").fetchone()
    conn.close()
    check("S10a 入库行带 salience_context 快照（非 NULL）",
          ctx_raw and ctx_raw[0] is not None, str(ctx_raw))
    ctx = json.loads(ctx_raw[0]) if ctx_raw and ctx_raw[0] else {}
    check("S10b 五档契约键齐（resonance/pins/social_weight/ambient_quota/now_state）",
          {"resonance", "pins", "social_weight", "ambient_quota", "now_state"} <= set(ctx),
          str(list(ctx)))
    others = ctx.get("resonance", {}).get("others", [])
    check("S10c others 架真命中种子画像（琴师·真复用 salience_read）",
          any("琴师" in str(o) for o in others), json.dumps(others, ensure_ascii=False))

    print(f"\n═══ 结果: {len(PASSES)} 过 / {len(FAILS)} 挂 ═══")
    if FAILS:
        for f in FAILS:
            print(f"  ❌ {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
