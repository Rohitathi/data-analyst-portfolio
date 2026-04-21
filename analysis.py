import pandas as pd
df = pd.read_csv('../data/cleaned_sales.csv')
print(df.groupby('Product')['Sales'].sum().sort_values(ascending=False))
