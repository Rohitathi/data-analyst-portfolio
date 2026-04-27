import pandas as pd
import os

print("Starting ETL pipeline in Docker...")

url = "https://raw.githubusercontent.com/plotly/datasets/master/sales_success.csv"
df = pd.read_csv(url)
print(f"Extracted {len(df)} rows")

df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
df = df.dropna()
print(f"Transformed {len(df)} rows")

assert len(df) > 0, "Failed: empty dataset"
assert df.isnull().sum().sum() == 0, "Failed: nulls found"
print("Quality checks passed!")

os.makedirs("/app/output", exist_ok=True)
df.to_csv("/app/output/sales_clean.csv", index=False)
print(f"Loaded {len(df)} rows to output/sales_clean.csv")
print("Pipeline complete!")
