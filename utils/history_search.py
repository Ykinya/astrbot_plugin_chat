"""SQLite FTS5 search for chat history.

The search index is kept in AstrBot's SQLite database.  It indexes a plain
text projection of ``platform_message_history.content`` so Chinese keyword
search does not have to scan the entire JSON table on every request.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from astrbot.api import logger


INDEX_TABLE = "chat_history_fts"
INDEX_STATE = "chat_history_fts_state"
DEFAULT_INDEX_LIMIT = 50000
DEFAULT_DAYS = 30
DEFAULT_CANDIDATES = 200
DEFAULT_RESULTS = 18
SOFT_CHARS = 10000
HARD_CHARS = 15000
CONTEXT_RADIUS = 2
CACHE_SECONDS = 600
INDEX_VERSION = 2
INDEX_INTERVAL_SECONDS = 600

_cache: dict[str, tuple[float, str]] = {}
_index_task: asyncio.Task | None = None


def database_path() -> Path:
    """Return the active AstrBot SQLite database path."""
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path

        return Path(get_astrbot_data_path()) / "data_v4.db"
    except Exception:
        root = os.environ.get("ASTRBOT_ROOT", "/AstrBot")
        return Path(root) / "data" / "data_v4.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(database_path()), timeout=30)
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def extract_text(content: Any) -> str:
    """Extract readable text from either AstrBot content JSON shape."""
    try:
        data = json.loads(content) if isinstance(content, str) else content
    except Exception:
        return str(content or "").strip()

    parts = data.get("message", []) if isinstance(data, dict) else data
    if not isinstance(parts, list):
        return ""

    def walk(value: Any, depth: int = 0) -> list[str]:
        if depth > 8:
            return []
        if isinstance(value, list):
            found: list[str] = []
            for item in value:
                found.extend(walk(item, depth + 1))
            return found
        if not isinstance(value, dict):
            return []

        kind = str(value.get("type") or value.get("mtype") or "").casefold()
        payload = value.get("data") if isinstance(value.get("data"), dict) else value
        if kind in {"plain", "text"}:
            text = str(payload.get("text") or payload.get("message") or "").strip()
            return [text] if text else []
        if kind == "at":
            name = str(
                payload.get("name")
                or payload.get("qq")
                or payload.get("user_id")
                or value.get("qq")
                or ""
            ).strip()
            return [f"@{name}"] if name else []
        if kind == "reply":
            sender = str(
                payload.get("sender_name") or payload.get("sender_nickname") or ""
            ).strip()
            text = str(payload.get("text") or payload.get("message_str") or "").strip()
            return [f"[引用 {sender}: {text}]".strip()] if sender or text else []
        if kind == "image":
            return ["[图片]"]

        if kind in {"node", "nodes", "forward", "forward_message", "json"}:
            nested = (
                payload.get("content")
                or payload.get("message")
                or payload.get("nodes")
                or payload.get("messages")
            )
            inner = " ".join(walk(nested, depth + 1)).strip()
            if not inner:
                return []
            nickname = str(payload.get("nickname") or payload.get("sender_name") or "").strip()
            user_id = str(payload.get("user_id") or payload.get("sender_id") or "").strip()
            who = f"{nickname}({user_id})" if nickname and user_id else nickname or user_id
            return [f"[转发消息 {who}: {inner}]" if who else f"[转发消息: {inner}]"]
        return []

    out = walk(parts)
    return " ".join(out).strip()


def _set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        f"INSERT INTO {INDEX_STATE}(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def _get_state(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute(f"SELECT value FROM {INDEX_STATE} WHERE key=?", (key,)).fetchone()
    return str(row[0]) if row else default


def ensure_index(limit: int = DEFAULT_INDEX_LIMIT) -> int:
    """Create and incrementally update the FTS5 trigram index.

    Returns the number of newly indexed source rows.
    """
    conn = _connect()
    inserted = 0
    try:
        conn.execute(
            f"CREATE VIRTUAL TABLE IF NOT EXISTS {INDEX_TABLE} USING fts5("
            "text, platform_id UNINDEXED, user_id UNINDEXED, sender_id UNINDEXED, "
            "sender_name UNINDEXED, created_at UNINDEXED, tokenize='trigram')"
        )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {INDEX_STATE}("
            "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )

        stored_version = _get_state(conn, "index_version", "0")
        if stored_version != str(INDEX_VERSION):
            conn.execute(f"DELETE FROM {INDEX_TABLE}")
            _set_state(conn, "last_source_id", "0")
            _set_state(conn, "index_version", str(INDEX_VERSION))
            conn.commit()

        last_id = int(_get_state(conn, "last_source_id", "0") or 0)
        fts_max = conn.execute(f"SELECT max(rowid) FROM {INDEX_TABLE}").fetchone()[0]
        if fts_max and int(fts_max) > last_id:
            last_id = int(fts_max)

        rows = conn.execute(
            "SELECT id, platform_id, user_id, sender_id, sender_name, created_at, content "
            "FROM platform_message_history WHERE id > ? ORDER BY id ASC LIMIT ?",
            (last_id, max(100, int(limit))),
        ).fetchall()
        for row in rows:
            source_id, platform_id, user_id, sender_id, sender_name, created_at, content = row
            text = extract_text(content)
            if not text:
                continue
            conn.execute(
                f"INSERT OR REPLACE INTO {INDEX_TABLE}"
                "(rowid, text, platform_id, user_id, sender_id, sender_name, created_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?)",
                (
                    int(source_id),
                    text,
                    str(platform_id or ""),
                    str(user_id or ""),
                    str(sender_id or ""),
                    str(sender_name or ""),
                    str(created_at or ""),
                ),
            )
            inserted += 1
            last_id = int(source_id)

        if rows:
            _set_state(conn, "last_source_id", str(last_id))

        # Periodically remove FTS rows whose source message was pruned.
        last_cleanup = float(_get_state(conn, "last_cleanup", "0") or 0)
        if time.time() - last_cleanup > 3600:
            conn.execute(
                f"DELETE FROM {INDEX_TABLE} WHERE rowid IN ("
                f"SELECT f.rowid FROM {INDEX_TABLE} f "
                "LEFT JOIN platform_message_history p ON p.id=f.rowid "
                "WHERE p.id IS NULL LIMIT 2000)"
            )
            _set_state(conn, "last_cleanup", str(time.time()))
        conn.commit()
        return inserted
    finally:
        conn.close()


async def start_indexer() -> None:
    """Warm the index in the background without blocking plugin startup."""
    global _index_task
    if _index_task and not _index_task.done():
        return

    async def worker() -> None:
        while True:
            try:
                made = await asyncio.to_thread(ensure_index)
                if made:
                    logger.info(f"[chat-history-search] 新增索引 {made} 条")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"[chat-history-search] 更新索引失败: {exc}")
            await asyncio.sleep(INDEX_INTERVAL_SECONDS)

    _index_task = asyncio.create_task(worker())


async def stop_indexer() -> None:
    global _index_task
    task = _index_task
    _index_task = None
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def _fts_expression(query: str) -> str:
    terms = [t for t in re.split(r"\s+", query.strip()) if t]
    expr = []
    for term in terms:
        escaped = term.replace('"', '""')
        expr.append(f'"{escaped}"')
    return " AND ".join(expr)


def _recent_sql(days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=max(1, min(365, int(days))))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _format_line(row: sqlite3.Row | tuple, text: str) -> str:
    created_at, sender_name, sender_id = str(row[0] or ""), str(row[1] or "未知"), str(row[2] or "")
    who = f"{sender_name}({sender_id})" if sender_id else sender_name
    return f"[{created_at}] {who}: {text}".strip()


def _search_candidates_sync(
    *,
    query: str,
    platform_id: str,
    scopes: list[str],
    days: int,
    all_groups: bool,
    candidates: int,
    final_limit: int,
) -> list[dict[str, Any]]:
    conn = _connect()
    conn.row_factory = sqlite3.Row
    try:
        cutoff = _recent_sql(days)
        scope_sql = ""
        params: list[Any] = []
        if not all_groups:
            if not scopes:
                return []
            placeholders = ",".join("?" for _ in scopes)
            scope_sql = f" AND p.user_id IN ({placeholders})"
            params.extend(scopes)

        expr = _fts_expression(query)
        rows: list[sqlite3.Row]
        if len(query.strip()) >= 3 and expr:
            rows = conn.execute(
                f"SELECT p.id, p.created_at, p.sender_id, p.sender_name, p.user_id, "
                f"bm25({INDEX_TABLE}) AS bm25_score "
                f"FROM {INDEX_TABLE} JOIN platform_message_history p "
                f"ON p.id={INDEX_TABLE}.rowid "
                f"WHERE {INDEX_TABLE} MATCH ? AND p.created_at >= ?{scope_sql} "
                f"ORDER BY bm25({INDEX_TABLE}) LIMIT ?",
                [expr, cutoff, *params, max(20, min(500, candidates))],
            ).fetchall()
        else:
            rows = []

        # Two-character Chinese keywords are valid user searches but below the
        # trigram minimum.  Use a bounded LIKE fallback for those cases.
        if not rows:
            like = f"%{query.strip()}%"
            like_scope = "" if all_groups else scope_sql
            like_params = params if not all_groups else []
            rows = conn.execute(
                f"SELECT p.id, p.created_at, p.sender_id, p.sender_name, p.user_id, 0.0 AS bm25_score "
                f"FROM {INDEX_TABLE} f JOIN platform_message_history p ON p.id=f.rowid "
                f"WHERE p.created_at >= ? AND f.text LIKE ?"
                f"{like_scope} ORDER BY p.id DESC LIMIT ?",
                [cutoff, like, *like_params, max(20, min(500, candidates))],
            ).fetchall()

        now = datetime.now(timezone.utc)
        now_naive = now.replace(tzinfo=None)
        ranked: list[dict[str, Any]] = []
        lowered = query.casefold()
        for row in rows:
            created = str(row["created_at"] or "")
            try:
                age_days = max(0.0, (now_naive - datetime.fromisoformat(created)).total_seconds() / 86400)
            except Exception:
                age_days = float(days)
            bm25_score = -float(row["bm25_score"] or 0.0)
            recency = max(0.0, 1.0 - age_days / max(1, days)) * 1.5
            sender = str(row["sender_name"] or "")
            sender_id = str(row["sender_id"] or "")
            sender_bonus = 2.0 if lowered and lowered in sender.casefold() else 0.0
            if sender_id and lowered == sender_id:
                sender_bonus = 3.0
            group_bonus = 1.0 if all_groups and str(row["user_id"]) in scopes else 0.0
            ranked.append(
                {
                    "id": int(row["id"]),
                    "created_at": created,
                    "sender_name": sender,
                    "sender_id": sender_id,
                    "user_id": str(row["user_id"] or ""),
                    "score": bm25_score + recency + sender_bonus + group_bonus,
                }
            )
        ranked.sort(key=lambda item: item["score"], reverse=True)
        return ranked[: max(1, min(20, final_limit))]
    finally:
        conn.close()


def _build_context_sync(
    selected: list[dict[str, Any]],
    *,
    radius: int,
    soft: int,
    hard: int,
) -> tuple[list[str], int]:
    conn = _connect()
    conn.row_factory = sqlite3.Row
    lines: list[str] = []
    seen: set[int] = set()
    used = 0
    radius = max(1, min(2, int(radius)))
    soft = max(1000, min(15000, int(soft)))
    hard = max(soft, min(15000, int(hard)))
    try:
        for hit in selected:
            source_id, scope = hit["id"], hit["user_id"]
            before = conn.execute(
                "SELECT id, created_at, sender_id, sender_name, content FROM platform_message_history "
                "WHERE user_id=? AND id<=? ORDER BY id DESC LIMIT ?",
                (scope, source_id, radius + 1),
            ).fetchall()
            after = conn.execute(
                "SELECT id, created_at, sender_id, sender_name, content FROM platform_message_history "
                "WHERE user_id=? AND id>? ORDER BY id ASC LIMIT ?",
                (scope, source_id, radius),
            ).fetchall()
            window: list[str] = []
            window_chars = 0
            for row in list(reversed(before)) + list(after):
                rid = int(row["id"])
                if rid in seen:
                    continue
                text = extract_text(row["content"])
                if not text:
                    continue
                line = _format_line((row["created_at"], row["sender_name"], row["sender_id"]), text)
                window.append(line)
                window_chars += len(line) + 1
            if not window:
                continue
            if used + window_chars <= soft:
                lines.extend(window)
                used += window_chars
            elif used + window_chars <= hard:
                lines.extend(window)
                used += window_chars
            else:
                continue
            for row in list(before) + list(after):
                seen.add(int(row["id"]))
            if used >= soft and len(lines) >= 15:
                break

        if not lines and selected:
            # Guarantee a useful response even if the first window is very long.
            first = selected[0]
            row = conn.execute(
                "SELECT id, created_at, sender_id, sender_name, content FROM platform_message_history WHERE id=?",
                (first["id"],),
            ).fetchone()
            if row:
                text = extract_text(row["content"])
                line = _format_line((row["created_at"], row["sender_name"], row["sender_id"]), text)
                lines.append(line[:hard])
                used = len(lines[0])
        return lines, used
    finally:
        conn.close()


def _search_sync(
    query: str,
    platform_id: str,
    scopes: list[str],
    days: int,
    all_groups: bool,
    candidates: int,
    final_limit: int,
    soft_chars: int,
    hard_chars: int,
    context_radius: int,
) -> str:
    ensure_index()
    selected = _search_candidates_sync(
        query=query,
        platform_id=platform_id,
        scopes=scopes,
        days=days,
        all_groups=all_groups,
        candidates=candidates,
        final_limit=final_limit,
    )
    if not selected:
        return json.dumps(
            {"ok": True, "query": query, "matched": 0, "characters": 0, "lines": []},
            ensure_ascii=False,
        )
    lines, used = _build_context_sync(
        selected,
        radius=context_radius,
        soft=soft_chars,
        hard=hard_chars,
    )
    return json.dumps(
        {
            "ok": True,
            "query": query,
            "days": days,
            "scope": "all_groups" if all_groups else "current_group",
            "matched": len(selected),
            "characters": used,
            "lines": lines,
            "instruction": "这是按关键词从 SQLite 检索出的历史片段，请基于原话总结，不要编造未出现的内容。",
        },
        ensure_ascii=False,
    )


async def search_for_event(
    event: Any,
    query: str,
    *,
    days: int = DEFAULT_DAYS,
    all_groups: bool = False,
    candidates: int = DEFAULT_CANDIDATES,
    final_limit: int = DEFAULT_RESULTS,
    soft_chars: int = SOFT_CHARS,
    hard_chars: int = HARD_CHARS,
    context_radius: int = CONTEXT_RADIUS,
    cache_seconds: int = CACHE_SECONDS,
) -> str:
    """Search history for the current event, with a short result cache."""
    keyword = " ".join(str(query or "").split()).strip()
    if not keyword:
        return "没有提供搜索关键词。"

    days = max(1, min(365, int(days or DEFAULT_DAYS)))
    final_limit = max(1, min(20, int(final_limit or DEFAULT_RESULTS)))
    soft_chars = max(1000, min(15000, int(soft_chars or SOFT_CHARS)))
    hard_chars = max(soft_chars, min(15000, int(hard_chars or HARD_CHARS)))
    context_radius = max(1, min(2, int(context_radius or CONTEXT_RADIUS)))
    cache_seconds = max(10, min(600, int(cache_seconds or CACHE_SECONDS)))
    platform_id = str(event.get_platform_id() or "")
    group_id = str(event.get_group_id() or "").strip()
    umo = str(getattr(event, "unified_msg_origin", "") or "").strip()
    scopes = [value for value in (group_id, umo) if value]
    admin = bool(getattr(event, "is_admin", lambda: False)())
    all_groups = bool(all_groups and admin)
    if not scopes and not all_groups:
        return "聊天历史搜索只能在群聊中，或由管理员搜索全部群。"

    cache_key = json.dumps(
        [platform_id, scopes, keyword, days, all_groups, candidates, final_limit, soft_chars, hard_chars, context_radius],
        ensure_ascii=False,
    )
    cached = _cache.get(cache_key)
    if cached and cached[0] > time.monotonic():
        return cached[1]

    result = await asyncio.to_thread(
        _search_sync,
        keyword,
        platform_id,
        scopes,
        days,
        all_groups,
        max(20, min(500, int(candidates or DEFAULT_CANDIDATES))),
        final_limit,
        soft_chars,
        hard_chars,
        context_radius,
    )
    now = time.monotonic()
    for old_key, (expiry, _) in list(_cache.items()):
        if expiry <= now:
            _cache.pop(old_key, None)
    if len(_cache) >= 64:
        _cache.pop(next(iter(_cache)), None)
    _cache[cache_key] = (now + cache_seconds, result)
    return result

