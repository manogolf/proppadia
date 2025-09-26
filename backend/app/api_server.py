from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import health, mlb, nhl

app = FastAPI(title="Proppadia Backend", version="0.1.0")

# CORS (adjust as needed)
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://www.proppadia.com",
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
app.include_router(nhl.router, prefix="/api")

@app.get("/")
def root():
    return {"service": "proppadia-backend", "status": "ok"}
