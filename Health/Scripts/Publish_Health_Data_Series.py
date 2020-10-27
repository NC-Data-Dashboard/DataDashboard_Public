import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

#### Backups ####
df_backup = pd.read_csv("./Updates/STG_WNCD_Health_Data_Series.txt", sep="\t")
df_backup.to_csv("./Backups/STG_WNCD_Health_Data_Series_BACKUP.txt", sep="\t")

#### COVID ####
df = pd.read_csv("./Updates/STG_NYTI_CNTY_COVID_19_Cases.txt", sep="\t")
df["Measure_Name"] = ""

df1 = pd.read_csv("./Updates/STG_NYTI_CNTY_COVID_19_Deaths.txt", sep="\t")
df1["Measure_Name"] = ""

df2 = pd.read_csv("./Updates/STG_NYTI_NAT_COVID_19_Cases.txt", sep="\t")
df2["Measure_Name"] = ""
df2["GeoArea_FIPS"] = "00000"

df3 = pd.read_csv("./Updates/STG_NYTI_NAT_COVID_19_Deaths.txt", sep="\t")
df3["Measure_Name"] = ""
df3["GeoArea_FIPS"] = "00000"

df_list = [df1, df2, df3]
df_append = df.append(df_list)

#### Save Data Series ####
df_append.to_csv("./Updates/STG_WNCD_Health_Data_Series.txt", sep="\t")
