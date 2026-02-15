from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# backend/app/api_server.py
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=False)  # load repo-root .env if present

from .routers import health, mlb, nhl, ops

app = FastAPI(title="Proppadia Backend", version="0.1.0")

REPO_ROOT = Path(__file__).resolve().parents[2]
NHL_SITE_DATA_DIR = REPO_ROOT / "nhl" / "site" / "data"

app.mount(
    "/nhl/site/data",
    StaticFiles(directory=str(NHL_SITE_DATA_DIR)),
    name="nhl_site_data",
)

# CORS (adjust as needed)
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://proppadia.com",
    "https://www.proppadia.com",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers under /api/*
app.include_router(health.router, prefix="/api")
app.include_router(mlb.router, prefix="/api")
app.include_router(nhl.router)
app.include_router(ops.router, prefix="/api")

@app.get("/")
def root():
    return {"service": "proppadia-backend", "status": "ok"}

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    p = REPO_ROOT / "frontend" / "public" / "favicon.ico"
    if p.exists():
        return FileResponse(p)
    return {}
