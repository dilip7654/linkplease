import os
from dotenv import load_dotenv

# Loads variables from a .env file in the project root into os.environ,
# if one exists. In production (Render/Railway/etc.) you'll set these as
# real environment variables in the platform's dashboard instead, and this
# call is just a harmless no-op there.
load_dotenv()

PSEUDOGRAM_BASE_URL = os.environ.get("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")
PSEUDOGRAM_API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "")

if not PSEUDOGRAM_API_KEY:
    # Fail loud, not silent. A missing key means every send call will 401/403
    # and you'll burn time debugging "why is nothing sending" instead of
    # seeing this immediately at startup.
    print("WARNING: PSEUDOGRAM_API_KEY is not set. Set it as an env var before running.")

# Rate limit the mock API enforces on us: 10 requests / rolling 60s.
# We stay under this on purpose rather than relying on retrying after 429s,
# because burning through 429s wastes attempts and adds latency for no reason.
RATE_LIMIT_MAX_REQUESTS = 10
RATE_LIMIT_WINDOW_SECONDS = 60