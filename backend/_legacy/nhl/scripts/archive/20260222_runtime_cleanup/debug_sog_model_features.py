from pathlib import Path
import joblib

# Adjust ROOT the same way cli.py does, just from /backend/nhl/scripts/
ROOT = Path(__file__).resolve().parents[3]  # repo root

model_root = ROOT / "backend" / "nhl" / "models" / "latest" / "shots_on_goal" / "sog_player_v2"

if not model_root.exists():
    raise SystemExit(f"Model root not found: {model_root}")

print(f"Inspecting models under: {model_root}")

for line_dir in sorted(model_root.iterdir()):
    if not line_dir.is_dir():
        continue

    lr_path = line_dir / "lr.joblib"
    if not lr_path.exists():
        print(f"[warn] {line_dir.name}: no lr.joblib, skipping")
        continue

    lr = joblib.load(lr_path)

    print("\n====================================")
    print(f"Line directory: {line_dir.name}  (line {line_dir.name.replace('_', '.')})")
    print("n_features_in_:", getattr(lr, "n_features_in_", None))

    feats = getattr(lr, "feature_names_in_", None)
    if feats is None:
        print("feature_names_in_: <missing on this model>")
    else:
        print("feature_names_in_:")
        for f in feats:
            print(f"  - {f}")
