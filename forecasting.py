import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

df = pd.read_csv('../data/cleaned_sales.csv')
df['Month'] = pd.to_datetime(df['Order_Date']).dt.month
monthly = df.groupby('Month')['Sales'].sum().reset_index()

X = monthly[['Month']]
y = monthly['Sales']

model = LinearRegression().fit(X, y)
print(model.predict(np.array([[13],[14]])))
