# backend/app/config.py

import os

# single source of truth used by all routes
COMMIT_TOKEN_SECRET = os.getenv("COMMIT_TOKEN_SECRET", "dev-secret")
COMMIT_TOKEN_TTL    = int(os.getenv("COMMIT_TOKEN_TTL", "600"))  # seconds
