import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json

# Load the JSON export file
with open("hits_props_export.json", "r") as f:
    data = json.load(f)

# Convert to DataFrame
df = pd.DataFrame(data)

# Print basic info
print("✅ Loaded data:", df.shape)
print(df.dtypes)

# Check for missing values
print("\n🧼 Missing values per column:")
print(df.isnull().sum())

# Check class distribution
print("\n📊 Outcome distribution:")
print(df["outcome"].value_counts())

# Ensure numeric types
df["prop_value"] = pd.to_numeric(df["prop_value"], errors="coerce")
df["rolling_result_avg_7"] = pd.to_numeric(df["rolling_result_avg_7"], errors="coerce")
df["opponent_avg_win_rate"] = pd.to_numeric(df["opponent_avg_win_rate"], errors="coerce").fillna(0.5)

# Create derived features
df["line_diff"] = df["rolling_result_avg_7"] - df["prop_value"]
df["opponent_encoded"] = df["opponent_avg_win_rate"]

# Clean rows where derived features and required inputs exist
feature_cols = ["line_diff", "hit_streak", "win_streak", "is_home", "opponent_encoded"]
df_clean = df.dropna(subset=feature_cols)


# Drop rows with missing required model features
feature_cols = ["line_diff", "hit_streak", "win_streak", "is_home", "opponent_encoded"]
df_clean = df.dropna(subset=feature_cols)

# Plot feature distributions
for col in feature_cols:
    plt.figure()
    sns.histplot(df_clean[col], bins=30, kde=True)
    plt.title(f"Distribution of {col}")
    plt.xlabel(col)
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(f"{col}_distribution.png")
    print(f"📈 Saved {col}_distribution.png")

# Correlation heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(df_clean[feature_cols].corr(), annot=True, cmap="coolwarm", fmt=".2f")
plt.title("🔗 Feature Correlation Heatmap")
plt.tight_layout()
plt.savefig("feature_correlation_heatmap.png")
print("📊 Saved feature_correlation_heatmap.png")
