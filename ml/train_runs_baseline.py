# ml/train_runs_baseline.py

import json
from pathlib import Path
import numpy as np
import pandas as pd

from pandas.api.types import is_numeric_dtype
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor as HGBR
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score, average_precision_score
from joblib import dump

# =========================
# Config
# =========================
ID_COLS = ["game_id", "player_id", "game_date"]
TARGET  = "y_runs"

CSV_PATH = Path(__file__).with_name("train_batter_runs.csv")
OUT_PATH = Path(__file__).with_name("pred_runs_test.csv")

LOOKBACK_DAYS = 540    # rolling window
TEST_DAYS     = 21
VAL_DAYS      = 14

models_dir = Path("ml/models")
models_dir.mkdir(parents=True, exist_ok=True)

# =========================
# Load & basic prep
# =========================
df = pd.read_csv(CSV_PATH)
df["game_date"] = pd.to_datetime(df["game_date"])
df = df.sort_values("game_date").reset_index(drop=True)

# coerce non-ID/non-target to numeric where possible
for c in df.columns:
    if c not in ID_COLS + [TARGET]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# apply lookback window
max_date = df["game_date"].max()
min_date = max_date - pd.Timedelta(days=LOOKBACK_DAYS)
df = df[df["game_date"] >= min_date].copy()

# features / target
feature_cols = [c for c in df.columns if c not in ID_COLS + [TARGET]]
X = df[feature_cols].copy()
y = df[TARGET].astype(float)
ids = df[ID_COLS].copy()

# cat vs num, simple imputes
num_cols = [c for c in X.columns if is_numeric_dtype(X[c])]
cat_cols = [c for c in X.columns if c not in num_cols]
for c in num_cols: X[c] = X[c].fillna(0.0)
for c in cat_cols: X[c] = X[c].fillna("UNK")

# =========================
# Time-based split
# =========================
test_start = max_date - pd.Timedelta(days=TEST_DAYS - 1)
val_start  = test_start - pd.Timedelta(days=VAL_DAYS)

train_idx = df["game_date"] < val_start
val_idx   = (df["game_date"] >= val_start) & (df["game_date"] < test_start)
test_idx  = df["game_date"] >= test_start

X_train, y_train = X[train_idx], y[train_idx]
X_val,   y_val   = X[val_idx],   y[val_idx]
X_test,  y_test  = X[test_idx],  y[test_idx]

print(f"Saved model trained on: {min_date.date()} → {max_date.date()}")
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
    loss="poisson",          # runs are counts ≥ 0
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

# =========================
# Eval helpers
# =========================
def eval_split(name, Xs, ys):
    pred = pipe.predict(Xs)
    yt = np.asarray(ys, dtype=float)
    yp = np.asarray(pred, dtype=float)
    err = yt - yp
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err**2)))
    ss_res = float(np.sum(err**2))
    ybar   = float(np.mean(yt))
    ss_tot = float(np.sum((yt - ybar)**2))
    r2 = float("nan") if ss_tot == 0.0 else 1.0 - (ss_res / ss_tot)
    print(f"{name:>5}  MAE={mae:.3f}  RMSE={rmse:.3f}  R2={r2:.3f}")
    return yp

_ = eval_split("train", X_train, y_train)
_ = eval_split(" val ", X_val,   y_val)
pred_test = eval_split("test",   X_test,  y_test)

# prevalence & constant Brier for line=0.5 (most common for Runs)
yt05 = (np.asarray(y_test) > 0.5).astype(int)
prev05 = float(np.mean(yt05))
brier_const05 = prev05 * (1.0 - prev05)
print(f"Test prevalence R≥0.5 = {prev05:.3f}  |  Constant baseline Brier = {brier_const05:.4f}")

# =========================
# Calibration bits
# =========================
def calibration_table(p, y, bins=10):
    edges = np.linspace(0.0, 1.0, bins + 1)
    rows = []
    for i in range(bins):
        lo, hi = edges[i], edges[i+1]
        m = (p >= lo) & (p < hi) if i < bins-1 else (p >= lo) & (p <= hi)
        if m.sum() == 0: 
            continue
        rows.append({"bin": f"[{lo:.1f},{hi:.1f}]", "n": int(m.sum()),
                     "p_hat": float(p[m].mean()), "p_emp": float(y[m].mean())})
    return rows

calibrators = {}

def calibrate_over_line(line, name=None):
    name = name or str(line).replace(".", "_")
    pv = pipe.predict(X_val)
    yv = (np.asarray(y_val) > line).astype(int)

    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(pv, yv)

    pt = pipe.predict(X_test)
    yt = (np.asarray(y_test) > line).astype(int)
    p_over = iso.predict(pt)

    brier = float(np.mean((p_over - yt)**2))
    auc   = float(roc_auc_score(yt, p_over))
    ap    = float(average_precision_score(yt, p_over))
    print(f"Line {line:>3}: Brier={brier:.4f}  ROC-AUC={auc:.3f}  PR-AUC={ap:.3f}")

    cal = calibration_table(p_over, yt, bins=10)
    print("  bin   n    p̂    p_obs")
    for r in cal:
        print(f"  {r['bin']:>9} {r['n']:4d}  {r['p_hat']:.3f}  {r['p_emp']:.3f}")

    calibrators[name] = {
        "line": float(line),
        "x": iso.X_thresholds_.tolist(),
        "y": iso.y_thresholds_.tolist(),
    }

    try:
        out[f"p_over_{name}"] = p_over
    except NameError:
        pass

    return p_over

# build output frame once (test rows)
if "out" not in locals():
    out = ids.loc[test_idx].copy()
    out["y_true"] = np.asarray(y_test)
    out["y_pred"] = np.asarray(pred_test)

# Calibrate common Run lines (mostly 0.5; 1.5 for fun)
for L in (0.5, 1.5):
    calibrate_over_line(L)

with open(models_dir / "rs_calibrators_v1.json", "w") as f:
    json.dump(calibrators, f, indent=2)
print(f"Saved calibrators to {models_dir/'rs_calibrators_v1.json'}")

# write predictions CSV
out.to_csv(OUT_PATH, index=False)
print(f"\nWrote {OUT_PATH} with calibrated probabilities for 0.5 / 1.5")

# =========================
# Final refit on train+val
# =========================
X_trval = pd.concat([X_train, X_val], axis=0)
y_trval = pd.concat([y_train, y_val], axis=0)
pipe.fit(X_trval, y_trval)
print("Refit complete (train+val).")

dump(pipe, models_dir / "rs_poisson_v1.joblib")
with open(models_dir / "rs_features_v1.json", "w") as f:
    json.dump(list(X_train.columns), f)

print(f"\nSaved model to {models_dir/'rs_poisson_v1.joblib'}")
print(f"Saved feature list to {models_dir/'rs_features_v1.json'}")
