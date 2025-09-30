# ml/train_singles_baseline.py

import json
from pathlib import Path
import os

import numpy as np
import pandas as pd
from joblib import dump
from pandas.api.types import is_numeric_dtype
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor as HGBR
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.isotonic import IsotonicRegression

# =========================
# Config
# =========================
LINE = 0.5  # common Singles line; we also calibrate 1.5 / 2.5 below
ID_COLS = ["game_id", "player_id", "game_date"]  # not used as features
TARGET = "y_singles"

CSV_PATH = Path(__file__).with_name("train_batter_singles.csv")
OUT_PATH = Path(__file__).with_name("pred_singles_test.csv")
TEST_DAYS = 21
VAL_DAYS  = 14

# Where to save models & calibrators
models_dir = Path("ml/models")
models_dir.mkdir(parents=True, exist_ok=True)

# =========================
# Load & prep
# =========================
df = pd.read_csv(CSV_PATH)
df["game_date"] = pd.to_datetime(df["game_date"])
df = df.sort_values("game_date").reset_index(drop=True)

# --- rolling lookback for daily retrains ---
LOOKBACK_DAYS = int(os.getenv("SINGLES_LOOKBACK_DAYS", "540"))  # ~18 months by default
max_date = df["game_date"].max()
min_keep = max_date - pd.Timedelta(days=LOOKBACK_DAYS - 1)
df = df[df["game_date"] >= min_keep].copy()

# Coerce all non-ID, non-target to numeric when possible (keeps this baseline dependency-light)
for c in df.columns:
    if c not in ID_COLS + [TARGET]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# Build features / target
feature_cols = [c for c in df.columns if c not in ID_COLS + [TARGET]]
X = df[feature_cols].copy()
y = df[TARGET].astype(float)

# Keep IDs for reporting
ids = df[ID_COLS].copy()

# Detect numerics vs categoricals
num_cols = [c for c in X.columns if is_numeric_dtype(X[c])]
cat_cols = [c for c in X.columns if c not in num_cols]

# Impute
for c in num_cols:
    X[c] = X[c].fillna(0.0)
for c in cat_cols:
    X[c] = X[c].fillna("UNK")

# =========================
# Time-based split
# =========================
max_date   = df["game_date"].max()
test_start = max_date - pd.Timedelta(days=TEST_DAYS - 1)
val_start  = test_start - pd.Timedelta(days=VAL_DAYS)

saved_min = df.loc[df["game_date"] < test_start, "game_date"].min()
saved_max = (test_start - pd.Timedelta(days=1))
print(f"Saved model trained on: {saved_min.date()} → {saved_max.date()}")

train_idx = df["game_date"] < val_start
val_idx   = (df["game_date"] >= val_start) & (df["game_date"] < test_start)
test_idx  = df["game_date"] >= test_start

X_train, y_train = X[train_idx], y[train_idx]
X_val,   y_val   = X[val_idx],   y[val_idx]
X_test,  y_test  = X[test_idx],  y[test_idx]

print("Date ranges:")
print(f"  train < {val_start.date()}  |  val [{val_start.date()}, {test_start.date()})  |  test ≥ {test_start.date()}")
print("Shapes:", X_train.shape, X_val.shape, X_test.shape)

# =========================
# Pipeline
# =========================
pre = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ("num", "passthrough", num_cols),
    ],
    remainder="drop",
)

model = HGBR(
    loss="poisson",         # singles count ≥ 0
    learning_rate=0.05,
    max_depth=None,
    max_leaf_nodes=31,
    min_samples_leaf=20,
    l2_regularization=0.0,
    early_stopping=False,
    random_state=42,
)

pipe = Pipeline([("pre", pre), ("hgb", model)])
pipe.fit(X_train, y_train)

# ---- feature importances (safe name extraction) ----
pre_fitted = pipe.named_steps["pre"]
hgb = pipe.named_steps["hgb"]

try:
    all_names = list(pre_fitted.get_feature_names_out())
except Exception:
    cat_names = []
    if len(cat_cols) > 0:
        ohe = pre_fitted.named_transformers_["cat"]
        if hasattr(ohe, "get_feature_names_out"):
            cat_names = list(ohe.get_feature_names_out(cat_cols))
    all_names = cat_names + list(num_cols)

imps = getattr(hgb, "feature_importances_", None)
if imps is not None and len(all_names) == len(imps):
    top = sorted(zip(all_names, imps), key=lambda x: x[1], reverse=True)[:30]
    print("\nTop 30 features:")
    for name, val in top:
        print(f"{name:40s} {val:.6f}")
else:
    print("\n(Skipping feature-importance table; feature names or counts not aligned.)")

# =========================
# Eval helpers
# =========================
def eval_split(name, Xs, ys):
    pred = pipe.predict(Xs)
    y_true = np.asarray(ys, dtype=float)
    y_pred = np.asarray(pred, dtype=float)

    err = y_true - y_pred
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err**2)))

    ss_res = float(np.sum(err**2))
    y_bar = float(np.mean(y_true))
    ss_tot = float(np.sum((y_true - y_bar) ** 2))
    r2 = float("nan") if ss_tot == 0.0 else 1.0 - (ss_res / ss_tot)

    print(f"{name:>5}  MAE={mae:.3f}  RMSE={rmse:.3f}  R2={r2:.3f}")
    return y_pred

_ = eval_split("train", X_train, y_train)
_ = eval_split(" val ", X_val, y_val)
pred_test = eval_split("test", X_test, y_test)

# Prevalence / constant Brier for LINE
ytL  = (np.asarray(y_test) > LINE).astype(int)
prev = float(np.mean(ytL))
brier_const = prev * (1.0 - prev)
print(f"Test prevalence Singles > {LINE} = {prev:.3f}  |  Constant baseline Brier = {brier_const:.4f}")

# =========================
# Calibration tools
# =========================
def calibration_table(p, y, bins=10):
    edges = np.linspace(0.0, 1.0, bins + 1)
    rows = []
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        m = (p >= lo) & (p < hi) if i < bins - 1 else (p >= lo) & (p <= hi)
        if m.sum() == 0:
            continue
        rows.append(
            {
                "bin": f"[{lo:.1f},{hi:.1f}]",
                "n": int(m.sum()),
                "p_hat": float(p[m].mean()),
                "p_emp": float(y[m].mean()),
            }
        )
    return rows

# collect isotonic calibrators here
calibrators = {}  # e.g. {"0_5": {"x":[...], "y":[...], "line":0.5}, ...}

def calibrate_over_line(line, name=None):
    name = name or str(line).replace(".", "_")

    # predictions on validation, build binary target for this line
    pv = pipe.predict(X_val)
    yv = (np.asarray(y_val) > line).astype(int)

    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(pv, yv)

    # test predictions and calibrated probabilities
    pt = pipe.predict(X_test)
    yt = (np.asarray(y_test) > line).astype(int)
    p_over = iso.predict(pt)  # in [0,1]

    # ---- metrics ----
    brier = float(np.mean((p_over - yt) ** 2))
    auc   = float(roc_auc_score(yt, p_over)) if len(np.unique(yt)) > 1 else float("nan")
    ap    = float(average_precision_score(yt, p_over)) if len(np.unique(yt)) > 1 else float("nan")
    print(f"Line {line:>3}: Brier={brier:.4f}  ROC-AUC={auc:.3f}  PR-AUC={ap:.3f}")

    # reliability table
    cal = calibration_table(p_over, yt, bins=10)
    print("  bin   n    p̂    p_obs")
    for r in cal:
        print(f"  {r['bin']:>9} {r['n']:4d}  {r['p_hat']:.3f}  {r['p_emp']:.3f}")

    # save calibrator breakpoints
    calibrators[name] = {
        "line": float(line),
        "x": iso.X_thresholds_.tolist(),
        "y": iso.y_thresholds_.tolist(),
    }

    # append to output if present
    col = f"p_over_{name}"
    try:
        out[col] = p_over
    except NameError:
        pass

    return p_over

# =========================
# Build 'out' once
# =========================
if "out" not in locals():
    out = ids.loc[test_idx].copy()
    out["y_true"] = np.asarray(y_test)
    out["y_pred"] = np.asarray(pred_test)

# Calibrate common Singles lines
for L in (0.5, 1.5, 2.5):
    calibrate_over_line(L)

with open(models_dir / "singles_calibrators_v1.json", "w") as f:
    json.dump(calibrators, f, indent=2)
print(f"Saved calibrators to {models_dir/'singles_calibrators_v1.json'}")

# Write predictions CSV
out.to_csv(OUT_PATH, index=False)
print(f"\nWrote {OUT_PATH} with calibrated probabilities for 0.5 / 1.5 / 2.5 (Singles)")

# =========================
# Save model EXACTLY as calibrated & evaluated (train-only fit)
# =========================
dump(pipe, models_dir / "singles_poisson_v1.joblib")
with open(models_dir / "singles_features_v1.json", "w") as f:
    json.dump(list(X.columns), f)
print(f"\nSaved model to {models_dir/'singles_poisson_v1.joblib'}")
print(f"Saved feature list to {models_dir/'singles_features_v1.json'}")
