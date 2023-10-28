import pandas as pd
import sqlalchemy.types

from env import DATABASE

df = pd.read_csv("../../../Данные/Данные по метеостанциям.csv")

df["time"] = pd.to_datetime(df["Местное время"], format="%d.%m.%Y %H:%M")
df = df.drop("Местное время", axis=1)

df.info()

dtypes_dict = dict[str]()
for float_column in df.select_dtypes(["float64"]).columns.values:
    dtypes_dict[float_column] = sqlalchemy.types.DECIMAL


from sqlalchemy import create_engine
engine = create_engine(DATABASE)

print("importing to database")
df.to_sql("weather", engine, if_exists="replace", index=False, dtype=dtypes_dict)
print("done")
