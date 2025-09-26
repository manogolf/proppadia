# File: backend/scripts/convert_json_to_csv.py

import pandas as pd
import sys
import os

def main():
    if len(sys.argv) != 3:
        print("Usage: python convert_json_to_csv.py <input_json> <output_csv>")
        sys.exit(1)

    input_json = sys.argv[1]
    output_csv = sys.argv[2]

    if not os.path.exists(input_json):
        print(f"❌ File not found: {input_json}")
        sys.exit(1)

    try:
        df = pd.read_json(input_json)
        df.to_csv(output_csv, index=False)
        print(f"✅ Converted {input_json} to {output_csv}")
    except Exception as e:
        print(f"❌ Error converting file: {e}")

if __name__ == "__main__":
    main()
