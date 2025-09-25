# backend/app/api_server.py

# 1 Load .env **before** importing any routes that read env at import-time
from pathlib import Path
from dotenv import load_dotenv
import os

_THIS = Path(__file__).resolve()
ROOT_ENV    = _THIS.parents[2] / ".env"   # repo root .env
BACKEND_ENV = _THIS.parents[1] / ".env"   # backend/.env

load_dotenv(ROOT_ENV, override=True)
load_dotenv(BACKEND_ENV, override=True)

# 2 Now import FastAPI and middleware
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# 3 Create the app (no lifespan, no prewarm)
app = FastAPI()

# 4 CORS so CRA can call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://proppadia.com",
        "https://www.proppadia.com",
    ],
    allow_origin_regex=r"https://.*\.proppadia\.com",
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# 5) Import routers AFTER app/env are set up
from app.routes.api.get_game_pk import router as get_game_pk_router
from app.routes.api.prepare_prop import router as prepare_router
from app.routes.api.predict import router as predict_router
from app.routes.api.players import router as players_router
from app.routes.api.player_profile import router as player_profile_router
from app.routes.api.props import router as props_router
from app.routes.api.score_props import router as score_props_router
from app.routes.admin import router as admin_router
# from app.routes.api.model_metrics import router as model_metrics_router
# from app.routes.api.user_vs_model_accuracy import router as user_vs_model_accuracy_router
# from app.routes.api.user_vs_model_accuracy_weekly import router as user_vs_model_weekly_router
# from app.routes.api.model_accuracy_weekly import router as model_accuracy_weekly_router
# from app.routes.api.player_list import router as player_list_router
# from app.routes.api.score_prop import router as score_prop_router

# 6) Register routes
app.include_router(get_game_pk_router, prefix="/api")
app.include_router(prepare_router, prefix="/api")
app.include_router(predict_router, prefix="/api")
app.include_router(players_router, prefix="/api")
app.include_router(player_profile_router, prefix="/api")
app.include_router(props_router, prefix="/api")
app.include_router(score_props_router, prefix="/api")
app.include_router(admin_router, prefix="/admin")
# app.include_router(model_metrics_router, prefix="/api")
# app.include_router(user_vs_model_accuracy_router, prefix="/api")
# app.include_router(user_vs_model_weekly_router, prefix="/api")
# app.include_router(model_accuracy_weekly_router, prefix="/api")
# app.include_router(player_list_router, prefix="/api")
# app.include_router(score_prop_router, prefix="/api")

# 7) Simple health check
@app.get("/healthz")
def healthz():
    return {"ok": True}
