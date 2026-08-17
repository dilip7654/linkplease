"""
SQLite setup. We use a single file-backed DB (not :memory:) so that a process
restart doesn't lose pending work — that's the #1 way this kind of system
loses DMs in production.

check_same_thread=False because we have multiple threads (the request handler
thread(s) FastAPI uses, plus our background worker/reconciler threads) all
touching the same file. We serialize access with a lock instead of relying on
one connection per thread, which keeps the code simple to reason about.
"""

import sqlite3
import threading

DB_PATH = "linkplease.db"

# One shared connection, guarded by a lock. SQLite handles concurrent readers
# fine, but writers need to be serialized — this lock does that for us instead
# of relying on SQLite's own busy-timeout/retry behavior.
_lock = threading.Lock()
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.row_factory = sqlite3.Row


def init_db():
    with _lock:
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS rules (
                rule_id     TEXT PRIMARY KEY,
                keyword     TEXT NOT NULL,
                dm_message  TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );

            -- Every webhook event we've ever seen, by event_id.
            -- event_id is the PRIMARY KEY, so "INSERT OR IGNORE" gives us
            -- free dedup on the ~8% of events that get redelivered.
            CREATE TABLE IF NOT EXISTS events (
                event_id    TEXT PRIMARY KEY,
                event_type  TEXT NOT NULL,
                comment_id  TEXT,
                received_at TEXT NOT NULL,
                raw_payload TEXT NOT NULL
            );

            -- Comments we've seen, so we can look up their text/user later
            -- and so comment.deleted events have something to mark deleted.
            CREATE TABLE IF NOT EXISTS comments (
                comment_id  TEXT PRIMARY KEY,
                post_id     TEXT,
                text        TEXT,
                user_id     TEXT,
                username    TEXT,
                created_at  TEXT,
                deleted_at  TEXT
            );

            -- The actual unit of work: "send this rule's message to this
            -- user". UNIQUE(rule_id, user_id) is what makes "never DM the
            -- same user twice for the same rule" true even if two matching
            -- comments race each other — SQLite rejects the second insert,
            -- we catch that and count it as a blocked duplicate.
            CREATE TABLE IF NOT EXISTS dm_tasks (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id         TEXT NOT NULL,
                user_id         TEXT NOT NULL,
                comment_id      TEXT NOT NULL,
                message         TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending',
                -- pending -> queued (202 from API) -> delivered/failed
                dm_id           TEXT,
                idempotency_key TEXT UNIQUE NOT NULL,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                UNIQUE(rule_id, user_id)
            );

            -- Simple counters table for numbers that aren't a status count
            -- on dm_tasks (duplicates_blocked isn't a task at all — it's a
            -- match we deliberately didn't create a task for).
            CREATE TABLE IF NOT EXISTS counters (
                name  TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO counters (name, value) VALUES ('duplicates_blocked', 0);
            """
        )
        _conn.commit()


def get_conn():
    """Every module imports this instead of opening its own connection."""
    return _conn


def get_lock():
    return _lock