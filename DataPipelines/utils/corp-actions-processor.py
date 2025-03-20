import pandas as pd 

df1 = pd.read_csv("dividend-2010-17.csv")
df2 = pd.read_csv("dividend-2018-25.csv")

df_combined = pd.concat([df1, df2], ignore_index=True)

df_combined.to_csv("dividend.csv")