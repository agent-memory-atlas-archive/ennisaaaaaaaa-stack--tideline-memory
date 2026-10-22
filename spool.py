#!/usr/bin/env python3
"""write-ahead intent spool — Tideline 写入排队暂存层 v0（纯函数，零 server 依赖）

设计正本: ~/plans/tideline-spool-design-v0.md
她 2026-10-23 拍板（原话）:
    "spool的技术实现按你的设计来就行啦，可以让照照帮忙审核实测，让琴师实测
     就可以，琴师有一个裸装tideline，环境安全，代码有问题也不会影响到你和
     照照的记忆。"
→ 发布梯子（从此固定）: 洄造(repo) → 照照审 → 琴师裸测 → prod。
本模块是第一 rung：纯函数层，不 import server、不被 server import（集成在
第二 rung，env 门 TIDELINE_SPOOL 缺省关=ponytail 门，旧路径零感知）。

三段（与堂屋 outbox 同构——信写进发件箱=已发出，drain 在后面异步代发）:
    enqueue(c, tool, args, created_by)   写入秒回小票，毫秒级，落盘即算数
    drain(c, apply_fn)                   单写者顺序搬票进主库，apply_fn 注入域逻辑
    pending_footer(c)                    读时合并 v0 = presence 合并（在途清单）

崩溃语义（本层的核心承诺）:
    apply 在单事务内完成【域插入 + 翻票状态】——apply_fn 被禁止自行 commit
    （事务边界归 drain）。崩溃/异常 → 整个 tx 回滚 → 域插入和翻票两头都不落
    → 小票仍 pending → 重放安全。不需要 narratives 加列做去重。
    毒票隔离：单票连续失败 3 次 → state='failed' 进隔离区，不堵后面的队
    （堂屋重投语义的裁剪版：failed 留表审计，人工 SQL 可捞回）。

数据语义注意（夹具数据注释义务）:
    write_spool.state ∈ {pending, applied, failed}；attempts 只在失败路累计。
"""
import hashlib
import json
import sqlite3


def ensure_tables(c: sqlite3.Connection) -> None:
    """幂等建表。老库补表零迁移（CREATE IF NOT EXISTS）。"""
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS write_spool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            intent TEXT NOT NULL,          -- {"tool": ..., "args": {...}} 信封
            text_hash TEXT NOT NULL,       -- intent 内容指纹（审计/排障用）
            created_by TEXT NOT NULL,      -- 壳/session 标识（审计：谁递的票）
            created_at TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending',
            applied_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        )
        """
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_spool_state ON write_spool(state, id)"
    )
    c.commit()


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _hash_intent(tool: str, args: dict) -> str:
    canonical = json.dumps({"tool": tool, "args": args},
                           ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def enqueue(c: sqlite3.Connection, tool: str, args: dict,
            created_by: str = "unknown", now: str | None = None) -> dict:
    """落小票，秒回回执。落盘即算数（commit 在这层）——回执=投递成功。

    注意：v0 不对 text_hash 加 UNIQUE——同一内容合法地写两笔是允许的
    （比如真重复的日常记忆）；hash 是审计指纹不是去重键。
    """
    ensure_tables(c)
    ts = now or _now()
    cur = c.execute(
        "INSERT INTO write_spool(intent, text_hash, created_by, created_at, state)"
        " VALUES(?,?,?,?, 'pending')",
        (json.dumps({"tool": tool, "args": args}, ensure_ascii=False),
         _hash_intent(tool, args), created_by, ts),
    )
    c.commit()
    row = c.execute(
        "SELECT COUNT(*) AS n FROM write_spool WHERE state='pending'"
    ).fetchone()
    return {
        "ticket_id": cur.lastrowid,
        "in_flight": row["n"] if isinstance(row, sqlite3.Row) else row[0],
        "state": "pending",
    }


def pending_count(c: sqlite3.Connection) -> int:
    try:
        row = c.execute(
            "SELECT COUNT(*) AS n FROM write_spool WHERE state='pending'"
        ).fetchone()
        return row["n"] if isinstance(row, sqlite3.Row) else row[0]
    except sqlite3.OperationalError:
        return 0  # 老库无表——读路径绝不因 spool 缺席而炸


def pending_footer(c: sqlite3.Connection, limit: int = 5) -> str:
    """读时合并 v0：presence 合并——在途清单附在读取结果尾。

    v0 只报「在途有什么」，不参与排序（语义合并留 v1 与照照评审定）。
    老库无表/空队 → 空串，读路径零感知。
    """
    try:
        rows = c.execute(
            "SELECT id, intent, created_by, created_at FROM write_spool"
            " WHERE state='pending' ORDER BY id LIMIT ?",
            (limit,),
        ).fetchall()
    except sqlite3.OperationalError:
        return ""
    total = pending_count(c)
    if not total:
        return ""
    lines = [f"⏳ 在途 {total} 条（spool 未落库，读时合并 v0=presence）:"]
    for r in rows:
        try:
            intent = json.loads(r["intent"])
            gesture = str(intent.get("args", {}).get("gesture", ""))[:40]
            tool = intent.get("tool", "?")
        except Exception:
            gesture, tool = "(intent 解析失败)", "?"
        lines.append(f"  #{r['id']} [{tool}] {gesture}… (by {r['created_by']})")
    if total > limit:
        lines.append(f"  …及其余 {total - limit} 条")
    return "\n".join(lines)


def drain(c: sqlite3.Connection, apply_fn, limit: int = 8,
          max_attempts: int = 3, now: str | None = None) -> dict:
    """单写者顺序搬票。apply_fn(c, intent_dict) 负责域插入，**禁止自行 commit**。

    事务边界归本函数：BEGIN IMMEDIATE → apply_fn 域插入 → 翻票 applied →
    COMMIT，一票一事务。异常 → ROLLBACK（域插入一并回滚）→ attempts+1。

    重试形状（phase24 首跑定形）：一票一轮只试一次。失败的头票**本轮让路**
    （游标跳过它继续搬后面的票），保持 pending 等下一轮 drain 重试——
    不在同一调用里对同票连环重试（那会让瞬态失败烧光三次机会、也堵不住
    队头）。连败 max_attempts 次（跨轮累计）→ state='failed' 隔离。
    """
    ts = now or _now()
    out = {"applied": [], "failed": [], "skipped_poison": 0}
    after_id = 0  # 本轮游标：失败让路后只看它后面的票
    for _ in range(limit):
        row = c.execute(
            "SELECT id, intent, attempts FROM write_spool"
            " WHERE state='pending' AND id > ? ORDER BY id LIMIT 1",
            (after_id,),
        ).fetchone()
        if row is None:
            break
        ticket_id, intent_json, attempts = row["id"], row["intent"], row["attempts"]
        try:
            intent = json.loads(intent_json)
        except Exception as e:  # 票本身坏了——直接隔离，不重试
            c.execute(
                "UPDATE write_spool SET state='failed', last_error=?, attempts=attempts+1"
                " WHERE id=?", (f"intent-unparseable: {e}", ticket_id))
            c.commit()
            out["failed"].append(ticket_id)
            after_id = ticket_id
            continue
        try:
            c.execute("BEGIN IMMEDIATE")
            apply_fn(c, intent)   # 域插入（不得 commit）
            c.execute(
                "UPDATE write_spool SET state='applied', applied_at=? WHERE id=?",
                (ts, ticket_id))
            c.commit()
            out["applied"].append(ticket_id)
        except Exception as e:
            try:
                c.rollback()
            except sqlite3.OperationalError:
                pass  # tx 已不在（极端崩溃窗）——继续走失败记账
            new_attempts = attempts + 1
            if new_attempts >= max_attempts:
                c.execute(
                    "UPDATE write_spool SET state='failed', attempts=?, last_error=?"
                    " WHERE id=?", (new_attempts, str(e)[:500], ticket_id))
                out["failed"].append(ticket_id)
            else:
                c.execute(
                    "UPDATE write_spool SET attempts=?, last_error=? WHERE id=?",
                    (new_attempts, str(e)[:500], ticket_id))
            c.commit()
            after_id = ticket_id  # 让路：本轮只搬它后面的票
    return out
