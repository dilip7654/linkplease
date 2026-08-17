import json
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel

from db import init_db, get_conn, get_lock
import pseudogram
from worker import start_background_threads, now_iso

app = FastAPI()


@app.on_event("startup")
def startup():
    init_db()
    start_background_threads()


class RuleIn(BaseModel):
    keyword: str
    dm_message: str


@app.post("/rules", status_code=201)
def create_rule(rule: RuleIn):
    conn = get_conn()
    lock = get_lock()
    rule_id = str(uuid.uuid4())
    with lock:
        conn.execute(
            "INSERT INTO rules (rule_id, keyword, dm_message, created_at) VALUES (?, ?, ?, ?)",
            (rule_id, rule.keyword, rule.dm_message, now_iso()),
        )
        conn.commit()
    return {"rule_id": rule_id, "keyword": rule.keyword, "dm_message": rule.dm_message}


@app.get("/stats")
def get_stats():
    conn = get_conn()
    lock = get_lock()
    with lock:
        sent = conn.execute(
            "SELECT COUNT(*) c FROM dm_tasks WHERE status='delivered'"
        ).fetchone()["c"]
        failed = conn.execute(
            "SELECT COUNT(*) c FROM dm_tasks WHERE status='failed'"
        ).fetchone()["c"]
        queued = conn.execute(
            "SELECT COUNT(*) c FROM dm_tasks WHERE status IN ('pending', 'queued')"
        ).fetchone()["c"]
        dup_row = conn.execute(
            "SELECT value FROM counters WHERE name='duplicates_blocked'"
        ).fetchone()
        duplicates_blocked = dup_row["value"] if dup_row else 0
    return {
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates_blocked,
    }


@app.post("/webhook")
async def webhook(request: Request):
    """
    Must return 200 fast. Everything here is local SQLite writes — no calls
    out to the DM-sending API happen in this request. Sending is entirely
    the background worker's job, so a slow or rate-limited send can never
    make us miss the 5-second window and start dropping events.
    """
    raw_body = await request.body()
    signature = request.headers.get("X-PseudoGram-Signature", "")

    if not pseudogram.verify_signature(raw_body, signature):
        # Reject forged requests (Part B). 401 rather than 200 so a bad
        # signature is visibly rejected, not silently swallowed.
        raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid json")

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    data = payload.get("data", {})

    conn = get_conn()
    lock = get_lock()

    with lock:
        # event_id is our PRIMARY KEY on `events` -> redelivered events
        # (same event_id, ~8% of the stream) get silently ignored here.
        cur = conn.execute(
            "INSERT OR IGNORE INTO events (event_id, event_type, comment_id, received_at, raw_payload) "
            "VALUES (?, ?, ?, ?, ?)",
            (event_id, event_type, data.get("comment_id"), now_iso(), raw_body.decode("utf-8")),
        )
        is_new_event = cur.rowcount > 0

        if is_new_event:
            if event_type == "comment.created":
                _handle_comment_created(conn, data)
            elif event_type == "comment.deleted":
                _handle_comment_deleted(conn, data)
        conn.commit()

    return {"status": "ok"}


def _handle_comment_created(conn, data):
    comment_id = data["comment_id"]
    text = data.get("text", "")
    user = data.get("from", {})
    user_id = user.get("user_id")
    username = user.get("username")

    conn.execute(
        """INSERT OR IGNORE INTO comments
           (comment_id, post_id, text, user_id, username, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (comment_id, data.get("post_id"), text, user_id, username, data.get("created_at")),
    )

    # Match against every rule whose keyword appears in the comment text,
    # case-insensitively, anywhere in the string.
    text_lower = text.lower()
    rules = conn.execute("SELECT * FROM rules").fetchall()
    for rule in rules:
        if rule["keyword"].lower() not in text_lower:
            continue

        idempotency_key = f"{rule['rule_id']}:{user_id}"
        try:
            conn.execute(
                """INSERT INTO dm_tasks
                   (rule_id, user_id, comment_id, message, status,
                    idempotency_key, attempts, next_attempt_at, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 'pending', ?, 0, ?, ?, ?)""",
                (
                    rule["rule_id"],
                    user_id,
                    comment_id,
                    rule["dm_message"],
                    idempotency_key,
                    now_iso(),
                    now_iso(),
                    now_iso(),
                ),
            )
        except Exception:
            # UNIQUE(rule_id, user_id) violation -> this user already has
            # a task (pending/sent/whatever) for this rule. That's exactly
            # what "duplicates_blocked" means: a DM we correctly chose not
            # to send.
            conn.execute(
                "UPDATE counters SET value = value + 1 WHERE name='duplicates_blocked'"
            )


def _handle_comment_deleted(conn, data):
    comment_id = data["comment_id"]
    conn.execute(
        "UPDATE comments SET deleted_at=? WHERE comment_id=?", (now_iso(), comment_id)
    )
    # If we have a pending (not-yet-sent) DM task tied to this comment,
    # cancel it — the comment that triggered it no longer exists. If it's
    # already queued/delivered, we leave it: the DM already went out (or is
    # in flight) and pulling it back isn't something the API supports.
    conn.execute(
        "UPDATE dm_tasks SET status='cancelled', updated_at=? WHERE comment_id=? AND status='pending'",
        (now_iso(), comment_id),
    )