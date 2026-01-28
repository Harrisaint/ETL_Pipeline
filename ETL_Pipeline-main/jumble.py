import pandas as pd
import numpy as np
import os

# 1. Use the absolute path you provided
input_path = r"C:\Users\hgmar\Downloads\ETL_Pipeline-main\ETL_Pipeline-main\human_vital_signs_dataset_2024.csv"
output_path = r"C:\Users\hgmar\Downloads\ETL_Pipeline-main\ETL_Pipeline-main\messy_health_data.csv"

print("--- Script Starting ---")

try:
    # 2. Attempt to load the data
    print(f"Step 1: Loading file from {input_path}...")
    df = pd.read_csv(input_path)
    print(f"Step 1 Success: Loaded {len(df)} rows.")

    # 3. Inject Messy Data (NaNs)
    print("Step 2: Injecting missing values (NaNs)...")
    for col in df.columns:
        # Randomly set 10% of each column to NaN
        df.loc[df.sample(frac=0.1).index, col] = np.nan
    print("Step 2 Success.")

    # 4. Inject Duplicates
    print("Step 3: Creating duplicate rows...")
    df = pd.concat([df, df.sample(frac=0.05)], ignore_index=True)
    print("Step 3 Success.")

    # 5. Save the messy file
    print(f"Step 4: Saving messy file to {output_path}...")
    df.to_csv(output_path, index=False)
    
    print("\n---------------------------------------")
    print("FINAL SUCCESS: Data jumbled!")
    print(f"Check your folder for: messy_health_data.csv")
    print("---------------------------------------")

except FileNotFoundError:
    print(f"\nERROR: File not found at {input_path}")
    print("Double check that the filename in your Downloads folder ends in .csv")
except Exception as e:
    print(f"\nAN ERROR OCCURRED: {e}")