"""
Everything that talks to the mock Instagram API lives here, so the rest of
the codebase never has to know about HTTP status codes or HMAC details.
"""

import hashlib
import hmac
import httpx

from config import PSEUDOGRAM_BASE_URL, PSEUDOGRAM_API_KEY

_client = httpx.Client(base_url=PSEUDOGRAM_BASE_URL, timeout=10.0)


def verify_signature(raw_body: bytes, signature_header: str) -> bool:
    """
    signature_header looks like 'sha256=<hex>'. We recompute the HMAC over
    the RAW body bytes (not the parsed JSON — re-serializing JSON can change
    whitespace/key order and produce a different signature) using our API
    key as the secret, then compare with hmac.compare_digest to avoid
    leaking timing information about how much of the signature matched.
    """
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = signature_header.split("=", 1)[1]
    computed = hmac.new(
        PSEUDOGRAM_API_KEY.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, computed)


def send_dm(recipient_user_id: str, message: str, comment_id: str, idempotency_key: str):
    """
    Returns (status_code, json_body). We don't raise on non-2xx here — the
    caller (worker.py) needs to see 429/500/400 and decide what to do with
    each, so raising and catching would just mean unwrapping it again.
    """
    resp = _client.post(
        "/v1/dm/send",
        headers={
            "X-API-Key": PSEUDOGRAM_API_KEY,
            "Idempotency-Key": idempotency_key,
        },
        json={
            "recipient_user_id": recipient_user_id,
            "message": message,
            "comment_id": comment_id,
        },
    )
    try:
        body = resp.json()
    except Exception:
        body = {}
    return resp.status_code, body


def get_dm_status(dm_id: str):
    resp = _client.get(f"/v1/dm/{dm_id}", headers={"X-API-Key": PSEUDOGRAM_API_KEY})
    try:
        body = resp.json()
    except Exception:
        body = {}
    return resp.status_code, body