#!/bin/bash

MODEL_DIR="/Users/jerrystrain/Projects/baseball-streaks/models"

cd "$MODEL_DIR" || exit 1

# Step 1: Replace '+' with '_' in filenames and remove redundant substrings
for file in *.pkl; do
    new_name=$(echo "$file" | sed 's/+/ /g' | tr -s ' ' '_' | sed 's/_with_streaks_with_streaks_/_with_streaks_/g')
    if [[ "$file" != "$new_name" ]]; then
        echo "Renaming: $file → $new_name"
        mv "$file" "$new_name"
    fi
done

echo "✅ Model file renaming complete."
