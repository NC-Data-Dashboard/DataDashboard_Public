# Imports
import urllib
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import requests
from sqlalchemy import create_engine
import pyodbc

response = requests.get("https://apps.bea.gov/regional/zip/CAINC5N.zip")
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[0]) as csvfile:
    df = pd.read_csv(csvfile, encoding="ISO-8859-1", sep=",", low_memory=False)

df.tail(10)
df.drop(df.tail(4).index, inplace=True)

df["LineCode"] = df["LineCode"].astype(int)
df["GeoFIPS"] = df["GeoFIPS"].str.replace('"', "")
df.drop(
    ["TableName", "Region", "IndustryClassification", "Unit", "Description"],
    axis=1,
    inplace=True,
)

print("Updating National data.")

backup_df = pd.read_csv(
    "./Updates/STG_BEA_CA5N_National.txt", encoding="ISO-8859-1", sep="\t"
)
backup_df.to_csv("./Backups/STG_BEA_CA5N_National_BACKUP.txt")

linecodes = [
    10,
    20,
    30,
    35,
    50,
    60,
    70,
    81,
    82,
    90,
    100,
    200,
    300,
    400,
    500,
    600,
    700,
    800,
    900,
    1000,
    11000,
    1200,
    1300,
    1400,
    1500,
    1600,
    1700,
    1800,
    1900,
    2000,
    2001,
    2002,
    2010,
    2011,
    2012,
]
df = df[df["LineCode"].isin(linecodes)]

column_list = df.columns.values
for i in column_list:
    df.loc[df[i].isnull(), i] = 0

df = df.rename(columns={"GeoFIPS": "GeoArea_FIPS", "GeoName": "GeoArea_Name"})

df_melt = df.melt(
    id_vars=["GeoArea_FIPS", "GeoArea_Name", "LineCode"],
    var_name="Date",
    value_name="Published_Value",
)

df_melt = df_melt.set_index("GeoArea_FIPS")

df_melt["LineCode"] = df_melt["LineCode"].astype(str)

df_melt.to_csv("./Updates/STG_BEA_CA5N_National.txt", sep="\t")

print("Done.")
