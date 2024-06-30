import matplotlib.pyplot as plt
import pandas as pd
df=pd.read_csv("us-counties.csv")
df['date']=pd.to_datetime(df['date'])
df=df[df['fips']==6037]
print(df)
plt.plot(df['date'], df['cases'])
plt.show()