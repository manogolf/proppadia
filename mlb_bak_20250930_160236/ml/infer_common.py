import json, numpy as np, pandas as pd
from joblib import load

def _iso_predict(raw: np.ndarray, bp: dict) -> np.ndarray:
    x = np.asarray(bp["x"], dtype=float)
    y = np.asarray(bp["y"], dtype=float)
    raw = np.asarray(raw, dtype=float)
    raw = np.clip(raw, x[0], x[-1])
    idx = np.searchsorted(x, raw, side="right") - 1
    idx = np.clip(idx, 0, len(x) - 2)
    x0, x1 = x[idx], x[idx + 1]
    y0, y1 = y[idx], y[idx + 1]
    t = np.divide(raw - x0, (x1 - x0), out=np.zeros_like(raw), where=(x1 > x0))
    return y0 + t * (y1 - y0)

def load_bundle(kind: str, models_dir: str = "ml/models"):
    model = load(f"{models_dir}/{kind}_poisson_v1.joblib")
    feat  = json.load(open(f"{models_dir}/{kind}_features_v1.json"))
    cal   = json.load(open(f"{models_dir}/{kind}_calibrators_v1.json"))
    return model, feat, cal

def score(df_new: pd.DataFrame, kind: str, line: float, models_dir: str = "ml/models") -> np.ndarray:
    model, feat, cal = load_bundle(kind, models_dir)
    X = df_new.reindex(columns=feat, fill_value=np.nan).copy()
    for c in X.columns:
        if X[c].dtype == object:
            X[c] = X[c].fillna("UNK")
        else:
            X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0.0)
    raw = model.predict(X)
    key = str(line).replace(".", "_")
    return _iso_predict(raw, cal[key])
