import pandas as pd 

# bonus = pd.read_csv("../corporate_actions/bonus.csv")
# bonus['Ratio'] = bonus['Purpose'].str.split().str.get(2)

# print(bonus)
# bonus.to_csv("bonus.csv")

# dividend = pd.read_csv("../corporate_actions/dividend.csv")
# dividend['type'] = dividend['Purpose'].str.split(" -").str.get(0)
# dividend['dividend'] = dividend['Purpose'].str.split(" -").str.get(2)
# print(dividend)
# dividend.to_csv("dividend.csv")

splits = pd.read_csv("../corporate_actions/split.csv")
splits[]