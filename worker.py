"""
Two background loops, each in their own daemon thread:

  sender_loop      - picks up 'pending' dm_tasks whose next_attempt_at has
                      passed, and tries to send them via the mock API.
  reconciler_loop   - re-checks dm_tasks that got a 202 ('queued') to see if
                      they eventually landed as delivered/failed. This is
                      what catches the "202 now, silently failed later" case
                      the assignment specifically calls out.

Both respect a shared, in-process rate limiter: 10 requests / rolling 60s.
"""

import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone

from db import get_conn, get_lock
import pseudogram
from config import RATE_LIMIT_MAX_REQUESTS, RATE_LIMIT_WINDOW_SECONDS

MAX_ATTEMPTS = 6  # after this many failed attempts, we give up and mark 'failed'


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class RateLimiter:
    """
    Token-bucket-ish limiter tracking the timestamps of our last N requests
    in memory. Before every send, we drop timestamps older than the window,
    and if we're still at the cap we sleep until the oldest one ages out.

    Honest caveat (this belongs in FAILURES.md too): this is in-process
    memory. If you ever ran two instances of this app against the same API
    key, they wouldn't know about each other's requests and could jointly
    exceed the limit. A single instance, which is what we're deploying,
    is fine.
    """

    def __init__(self, max_requests, window_seconds):
        self.max_requests = max_requests
        self.window = timedelta(seconds=window_seconds)
        self.timestamps = deque()
        self.lock = threading.Lock()

    def wait_for_slot(self):
        while True:
            with self.lock:
                now = datetime.now(timezone.utc)
                while self.timestamps and now - self.timestamps[0] > self.window:
                    self.timestamps.popleft()
                if len(self.timestamps) < self.max_requests:
                    self.timestamps.append(now)
                    return
                sleep_for = (self.timestamps[0] + self.window - now).total_seconds()
            time.sleep(max(sleep_for, 0.05))


rate_limiter = RateLimiter(RATE_LIMIT_MAX_REQUESTS, RATE_LIMIT_WINDOW_SECONDS)


def _backoff_seconds(attempts: int) -> float:
    # Exponential backoff with a cap, so a task that keeps 500ing doesn't
    # get retried instantly forever: 2, 4, 8, 16, 32, capped at 60s.
    return min(2 ** attempts, 60)


def _attempt_send(task):
    conn = get_conn()
    lock = get_lock()

    rate_limiter.wait_for_slot()
    status_code, body = pseudogram.send_dm(
        recipient_user_id=task["user_id"],
        message=task["message"],
        comment_id=task["comment_id"],
        idempotency_key=task["idempotency_key"],
    )

    with lock:
        if status_code == 202:
            # Accepted, not delivered. We record the dm_id and move it to
            # 'queued' — the reconciler loop is responsible for finding out
            # whether it actually landed.
            conn.execute(
                """UPDATE dm_tasks
                   SET status='queued', dm_id=?, updated_at=?
                   WHERE id=?""",
                (body.get("dm_id"), now_iso(), task["id"]),
            )
        elif status_code == 429:
            # Shouldn't happen often since we self-limit, but if it does,
            # respect Retry-After-ish behavior: just push next_attempt_at
            # out and don't count it as a failed attempt against MAX_ATTEMPTS.
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=10)
            conn.execute(
                "UPDATE dm_tasks SET next_attempt_at=?, updated_at=? WHERE id=?",
                (retry_at.isoformat(), now_iso(), task["id"]),
            )
        elif status_code == 400:
            # Malformed request — retrying will never help, so we don't
            # burn attempts on it. Straight to failed.
            conn.execute(
                "UPDATE dm_tasks SET status='failed', attempts=attempts+1, updated_at=? WHERE id=?",
                (now_iso(), task["id"]),
            )
        else:
            # 500 or anything else unexpected: retryable, with backoff.
            attempts = task["attempts"] + 1
            if attempts >= MAX_ATTEMPTS:
                conn.execute(
                    "UPDATE dm_tasks SET status='failed', attempts=?, updated_at=? WHERE id=?",
                    (attempts, now_iso(), task["id"]),
                )
            else:
                retry_at = datetime.now(timezone.utc) + timedelta(
                    seconds=_backoff_seconds(attempts)
                )
                conn.execute(
                    """UPDATE dm_tasks
                       SET attempts=?, next_attempt_at=?, updated_at=?
                       WHERE id=?""",
                    (attempts, retry_at.isoformat(), now_iso(), task["id"]),
                )
        conn.commit()


def sender_loop(poll_interval=0.5):
    conn = get_conn()
    lock = get_lock()
    while True:
        with lock:
            row = conn.execute(
                """SELECT * FROM dm_tasks
                   WHERE status='pending' AND next_attempt_at <= ?
                   ORDER BY id LIMIT 1""",
                (now_iso(),),
            ).fetchone()
        if row is None:
            time.sleep(poll_interval)
            continue
        _attempt_send(row)


def reconciler_loop(poll_interval=2.0):
    """
    Picks up tasks sitting in 'queued' (i.e. the API accepted them but
    hasn't told us the final outcome) and polls GET /v1/dm/{dm_id}.
    Reads don't count against the rate limit, so no rate limiter needed here.
    """
    conn = get_conn()
    lock = get_lock()
    while True:
        with lock:
            rows = conn.execute(
                "SELECT * FROM dm_tasks WHERE status='queued' AND dm_id IS NOT NULL"
            ).fetchall()
        for task in rows:
            status_code, body = pseudogram.get_dm_status(task["dm_id"])
            if status_code != 200:
                continue
            remote_status = body.get("status")
            if remote_status == "delivered":
                with lock:
                    conn.execute(
                        "UPDATE dm_tasks SET status='delivered', updated_at=? WHERE id=?",
                        (now_iso(), task["id"]),
                    )
                    conn.commit()
            elif remote_status == "failed":
                # This is the "202 now, failed later" case. Re-queue it for
                # another send attempt rather than giving up immediately,
                # since the accepted-but-failed outcome is itself something
                # worth retrying (per Part C: "catch those and retry them").
                with lock:
                    attempts = task["attempts"] + 1
                    if attempts >= MAX_ATTEMPTS:
                        conn.execute(
                            "UPDATE dm_tasks SET status='failed', attempts=?, updated_at=? WHERE id=?",
                            (attempts, now_iso(), task["id"]),
                        )
                    else:
                        retry_at = datetime.now(timezone.utc) + timedelta(
                            seconds=_backoff_seconds(attempts)
                        )
                        conn.execute(
                            """UPDATE dm_tasks
                               SET status='pending', dm_id=NULL, attempts=?,
                                   next_attempt_at=?, updated_at=?
                               WHERE id=?""",
                            (attempts, retry_at.isoformat(), now_iso(), task["id"]),
                        )
                    conn.commit()
            # if 'queued' still, leave it and check again next poll
        time.sleep(poll_interval)


def start_background_threads():
    threading.Thread(target=sender_loop, daemon=True).start()
    threading.Thread(target=reconciler_loop, daemon=True).start()