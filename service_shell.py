#!/usr/bin/env python3
"""service_shell — Tideline 服务化 A 块：FastAPI 服务壳 v0。

spec: ~/plans/tideline-服务化-spec-v0.md（洄 2026-10-14，svps 正本）
定位（一句话）：把 Tideline 从被动库变成可部署服务的最小服务壳——
事件进（POST /event）、显著度出（GET /salience）、活着可探（GET /health）。

设计边界（照 spec 的非目标，一条不越）：
  - 不换存储：记忆库（mcp_memory.db）只读，一个字节不写；
    事件台账是本服务自己的 sidecar 库（默认 service_shell.db，与记忆库同目录）。
  - 不做高可用/多租户/UI。单进程单机，绑定 127.0.0.1。
  - 显著性引擎不重写：GET /salience 的背景材料复用 salience_read.salience_read()
    的只读检索；沉降路由是 v0 影子模式——只出 verdict（would_wake），
    不真写 wake_log、不真叫醒任何 thread。真唤醒等灰度第 3 步洄按按钮。

口的契约（沿用 spoor #344/#345 的 body 权威 + id 回显）：
  - POST /event  200 {"status":"ok"|"duplicate", "id": <int>, "wake":[...]}
      状态码只留粗语义；机器判别看 body.status。event_id 幂等去重。
      校验失败 422，body 带 "need" 字段名——报错报在人读得懂的地方（#754 教训）。
  - GET /salience?thread=<id>  单 thread 显著度账：score/decay 明细 + verdict
      不带 thread  → 全部 open threads 排行（C 块双跑对账的报表口）
  - GET /events?since_id=&limit=   事件台账增量读口（消费者/对账用）
  - GET /health                   活着可探：两库状态/事件数/最后事件 ts/降级位

迟到乱序容忍（spec §5 风险预记）：事件按自带 ts 存、按 ts 算衰减，
晚到的旧事件插进来不会污染新事件的窗口账；ts 解析不动时回落收件时刻并计数。

跑法（本地冒烟）:
  MEMORY_MCP_DB=<记忆库> TIDELINE_SERVICE_DB=<台账> \
    python3 -m uvicorn service_shell:app --host 127.0.0.1 --port 8890
测试: python3 tests/fixture_phase21_service_shell.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

# ── 配置（全部可拍板的旋钮，env 优先） ─────────────────────────────
MEMORY_DB = os.environ.get("MEMORY_MCP_DB", str(Path.home() / "memory" / "mcp_memory.db"))
EVENTS_DB = os.environ.get("TIDELINE_SERVICE_DB", str(Path(MEMORY_DB).parent / "service_shell.db"))
WAKE_THRESHOLD = float(os.environ.get("SERVICE_WAKE_THRESHOLD", "3.0"))   # 沉降路由唤醒线
WINDOW_HOURS = float(os.environ.get("SERVICE_WINDOW_HOURS", "48"))        # 显著度窗口
DECAY_HALF_LIFE_H = float(os.environ.get("SERVICE_DECAY_HOURS", "12"))    # 衰减半衰期
MAX_LIST = 500                                                            # 排行榜/事件口上限

_STARTED_AT = time.time()
_TS_COERCED = 0  # ts 解析失败回落收件时刻的计数（health 暴露，不静默）

EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS service_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT UNIQUE,             -- 调用方幂等键（如 moonshake row_id），缺省 NULL 不去重
  ts TEXT NOT NULL,                 -- 事件自带时刻（ISO；排序与衰减都用它）
  received_at TEXT NOT NULL,        -- 收件时刻（迟到/乱序审计用）
  organ_id TEXT NOT NULL,
  modality TEXT,
  event_type TEXT,
  saliency_hint REAL NOT NULL DEFAULT 1.0,
  payload_summary TEXT,
  event_json TEXT NOT NULL,          -- 原始信封全量，审计可回放
  salience_context TEXT              -- 入库时 salience_read.salience_context() 的背景材料快照
                                    -- （JSON 文本；NULL=入库时没调——审计可区分）
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON service_events(ts);
CREATE INDEX IF NOT EXISTS idx_events_organ ON service_events(organ_id);
"""


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_ts(raw: str | None) -> tuple[str, bool]:
    """把事件 ts 收敛成 aware ISO 串。解析不动→回落收件时刻，coerced=True。"""
    global _TS_COERCED
    if isinstance(raw, str) and raw.strip():
        try:
            dt = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat(timespec="seconds"), False
        except ValueError:
            pass
    _TS_COERCED += 1
    return _utcnow_iso(), True


@contextmanager
def _events_conn():
    Path(EVENTS_DB).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(EVENTS_DB, timeout=30)
    try:
        conn.executescript(EVENTS_SCHEMA)
        yield conn
        conn.commit()
    finally:
        conn.close()


def _memory_conn():
    """记忆库连接：只读意图。库不存在→返回 None（读侧降级成空，不炸——
    服务壳的活命不依赖记忆库在场，/events 台账始终可用）。"""
    if not Path(MEMORY_DB).exists():
        return None
    return sqlite3.connect(f"file:{MEMORY_DB}?mode=ro", uri=True, timeout=30)


def _open_threads(conn, limit: int = MAX_LIST) -> list[dict]:
    """open threads 只读拉取。无连接/表缺失→空 list（salience_read 同款降级约定）。"""
    if conn is None:
        return []
    try:
        rows = conn.execute(
            "SELECT id, content, importance, unresolved, weight, created_at "
            "FROM threads WHERE status='open' ORDER BY weight DESC, id LIMIT ?",
            (limit,),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [
        {"id": r[0], "content": r[1], "importance": r[2], "unresolved": r[3],
         "weight": r[4], "created_at": r[5]}
        for r in rows
    ]


def _decay(now: datetime, ev_ts: str) -> float:
    """指数衰减：age 负数（事件在未来/时钟歪）按 0 处理，不放大。"""
    try:
        dt = datetime.fromisoformat(ev_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age_h = max(0.0, (now - dt).total_seconds() / 3600.0)
    except ValueError:
        age_h = 0.0
    return 2.0 ** (-age_h / DECAY_HALF_LIFE_H)


def _events_in_window(econn: sqlite3.Connection, now: datetime) -> list[dict]:
    """窗口内事件按 ts 升序（乱序插入不影响读序）。"""
    since = datetime.fromtimestamp(
        now.timestamp() - WINDOW_HOURS * 3600, tz=timezone.utc
    ).isoformat(timespec="seconds")
    rows = econn.execute(
        "SELECT id, ts, organ_id, saliency_hint, payload_summary, event_json "
        "FROM service_events WHERE ts >= ? ORDER BY ts ASC, id ASC LIMIT ?",
        (since, MAX_LIST),
    ).fetchall()
    out = []
    for r in rows:
        try:
            env = json.loads(r[5])
        except json.JSONDecodeError:
            env = {}
        out.append({"id": r[0], "ts": r[1], "organ_id": r[2], "hint": r[3],
                    "payload_summary": r[4] or "", "entities": _entities(env)})
    return out


def _entities(env: dict) -> list[str]:
    ents = env.get("entities")
    if not isinstance(ents, (list, tuple)):
        return []
    return [e for e in ents if isinstance(e, str) and e.strip()]


def thread_salience(econn, mconn, thread: dict, now: datetime,
                    q: str | None = None) -> dict:
    """单 thread 显著度账：匹配=事件实体以子串出现在 thread content
    （或 ?q= 手动加一个 payload 关键词过滤器，C 块对账用）。
    score = Σ hint × decay × (1 + 0.25×min(实体命中数,2))。
    verdict 只出影子判定，不写任何库。"""
    content = thread.get("content") or ""
    matched, rows = [], _events_in_window(econn, now)
    for ev in rows:
        hits = [e for e in ev["entities"] if e in content]
        by_q = bool(q) and q in ev["payload_summary"]
        if not hits and not by_q:
            continue
        d = _decay(now, ev["ts"])
        contrib = ev["hint"] * d * (1.0 + 0.25 * min(len(hits), 2))
        matched.append({"event_id": ev["id"], "ts": ev["ts"], "organ": ev["organ_id"],
                        "hint": ev["hint"], "decay": round(d, 4),
                        "payload_summary": ev["payload_summary"],
                        "entity_hits": hits, "contrib": round(contrib, 4)})
    score = round(sum(m["contrib"] for m in matched), 4)
    return {"thread": thread, "score": score, "matched": matched,
            "threshold": WAKE_THRESHOLD, "window_h": WINDOW_HOURS,
            "verdict": "wake" if score >= WAKE_THRESHOLD else "quiet"}


def _bg_snapshot(env: dict) -> str | None:
    """入库时调 salience_read.salience_context() 存背景材料快照（复用，不重写）。

    任何异常（模块没加载/表缺失/脏数据）都降级成 None——事件入库永不为
    背景材料失败买单（spec：读口无状态纯函数语义，坏档可回放重算）。
    单文件部署兼容（server.py 同款）：模块不在 path 时把自身目录补进 sys.path。
    """
    try:
        try:
            import salience_read
        except ImportError:
            sys.path.insert(0, str(Path(__file__).resolve().parent))
            import salience_read
        conn = _memory_conn()
        if conn is None:
            return None
        try:
            return json.dumps(
                salience_read.salience_context(conn, env), ensure_ascii=False)
        finally:
            conn.close()
    except Exception:
        return None


# ── FastAPI 壳 ────────────────────────────────────────────────────
app = FastAPI(title="tideline-service-shell", version="0.1.0")


@app.get("/health")
def health():
    with _events_conn() as econn:
        n = econn.execute("SELECT COUNT(*), MAX(ts) FROM service_events").fetchone()
    mem_ok = Path(MEMORY_DB).exists()
    return {"ok": True, "service": "tideline-service-shell", "version": "0.1.0",
            "memory_db": MEMORY_DB, "memory_db_ok": mem_ok,
            "events_db": EVENTS_DB, "events_total": n[0] or 0,
            "last_event_ts": n[1], "ts_coerced": _TS_COERCED,
            "knobs": {"wake_threshold": WAKE_THRESHOLD, "window_h": WINDOW_HOURS,
                      "decay_half_life_h": DECAY_HALF_LIFE_H},
            "uptime_s": round(time.time() - _STARTED_AT, 1)}


@app.post("/event")
async def post_event(request: Request):
    try:
        env = await request.json()
    except Exception:
        return JSONResponse(status_code=422, content={"need": "json body", "status": "rejected"})
    if not isinstance(env, dict):
        return JSONResponse(status_code=422, content={"need": "json object body", "status": "rejected"})
    # 校验：报错报在人读得懂的地方（#754/#761 教训——字段名进 body）
    if not isinstance(env.get("ts"), str) or not env["ts"].strip():
        return JSONResponse(status_code=422, content={"need": "ts", "status": "rejected"})
    organ = env.get("organ_id")
    if not isinstance(organ, str) or not organ.strip():
        return JSONResponse(status_code=422, content={"need": "organ_id", "status": "rejected"})

    ts, coerced = _parse_ts(env.get("ts"))
    hint = env.get("saliency_hint", 1.0)
    try:
        hint = float(hint)
    except (TypeError, ValueError):
        hint = 1.0
    event_id = env.get("event_id") if isinstance(env.get("event_id"), str) and env["event_id"] else None

    with _events_conn() as econn:
        try:
            cur = econn.execute(
                "INSERT INTO service_events "
                "(event_id, ts, received_at, organ_id, modality, event_type, "
                " saliency_hint, payload_summary, event_json, salience_context) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (event_id, ts, _utcnow_iso(), organ.strip(), env.get("modality"),
                 env.get("event_type"), hint, env.get("payload_summary"),
                 json.dumps(env, ensure_ascii=False), _bg_snapshot(env))
            )
        except sqlite3.IntegrityError:
            # 幂等重发也算确认——回显已存行的 id（#345 id 回显契约，duplicate 不豁免）
            exist = econn.execute(
                "SELECT id FROM service_events WHERE event_id=?", (event_id,)
            ).fetchone()
            return {"status": "duplicate", "id": exist[0] if exist else None,
                    "ts_coerced": coerced, "wake": []}
        new_id = cur.lastrowid or 0
        # 影子沉降路由：只算 verdict 回执，不写记忆库不叫醒任何人
        now = datetime.now(timezone.utc)
        mconn = _memory_conn()
        try:
            wakes = []
            for t in _open_threads(mconn):
                acct = thread_salience(econn, mconn, t, now)
                if acct["verdict"] == "wake":
                    wakes.append({"thread_id": t["id"], "score": acct["score"],
                                  "matched_events": len(acct["matched"])})
        finally:
            if mconn is not None:
                mconn.close()
    return {"status": "ok", "id": new_id, "ts_coerced": coerced, "wake": wakes}


@app.get("/salience")
def salience(thread: int | None = Query(None), q: str | None = Query(None),
             now: str | None = Query(None)):
    """单 thread 显著度账；不带 thread 给 open threads 排行（对账报表口）。"""
    try:
        now_dt = datetime.fromisoformat(_parse_ts(now)[0]) if now else datetime.now(timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        now_dt = datetime.now(timezone.utc)
    with _events_conn() as econn:
        mconn = _memory_conn()
        try:
            threads = _open_threads(mconn)
            if thread is not None:
                hit = next((t for t in threads if t["id"] == thread), None)
                if hit is None:
                    return JSONResponse(status_code=404,
                                        content={"need": f"open thread id={thread}",
                                                 "status": "not_found"})
                return thread_salience(econn, mconn, hit, now_dt, q=q)
            accounts = [thread_salience(econn, mconn, t, now_dt, q=q) for t in threads]
            accounts.sort(key=lambda a: a["score"], reverse=True)
            return {"threads": accounts[:50], "memory_db_ok": Path(MEMORY_DB).exists()}
        finally:
            if mconn is not None:
                mconn.close()


@app.get("/events")
def events(since_id: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=MAX_LIST)):
    with _events_conn() as econn:
        rows = econn.execute(
            "SELECT id, event_id, ts, received_at, organ_id, modality, event_type, "
            "saliency_hint, payload_summary FROM service_events "
            "WHERE id > ? ORDER BY id ASC LIMIT ?",
            (since_id, limit),
        ).fetchall()
    items = [{"id": r[0], "event_id": r[1], "ts": r[2], "received_at": r[3],
              "organ_id": r[4], "modality": r[5], "event_type": r[6],
              "saliency_hint": r[7], "payload_summary": r[8]} for r in rows]
    return {"events": items, "count": len(items),
            "next_since_id": items[-1]["id"] if items else since_id}


if __name__ == "__main__":
    # 直跑模式：不装 uvicorn 的机器上 python3 service_shell.py 也能起（本地冒烟用）
    import uvicorn
    uvicorn.run(app, host=os.environ.get("SERVICE_HOST", "127.0.0.1"),
                port=int(os.environ.get("SERVICE_PORT", "8890")))
