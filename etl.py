import pandas as pd
df = pd.read_csv('../data/cleaned_sales.csv')
summary = df.groupby('Category').agg({'Sales':'sum','Profit':'sum'}).reset_index()
summary.to_csv('../data/category_summary.csv', index=False)
print("ETL done")
