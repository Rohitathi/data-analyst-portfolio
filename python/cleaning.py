import pandas as pd
df = pd.read_csv('../data/raw_sales.csv')
df['Order_Date'] = pd.to_datetime(df['Order_Date'])
df = df.drop_duplicates()
df['Sales'] = df['Sales'].fillna(df['Sales'].median())
df['Profit'] = df['Profit'].fillna(0)
df = df[df['Sales'] > 0]
df.to_csv('../data/cleaned_sales.csv', index=False)
print("Cleaning done")
