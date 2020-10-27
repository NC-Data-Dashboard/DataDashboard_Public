import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

# create backups
df_backup = pd.read_csv("./Updates/STG_NYTI_NAT_COVID_19_Deaths.txt", sep="\t")
df_backup.to_csv("./Backups/STG_NYTI_NAT_COVID_19_Deaths_BACKUP.txt", sep="\t")

# read data
df = pd.read_csv("./Data/covid-19-data/us.csv")

# clean
df = df.rename(columns={"date": "Data_Period_Business_Key"})
df["Data_Period_Business_Key"] = pd.to_datetime(df["Data_Period_Business_Key"])
df["Estimated_Value"] = df["deaths"].astype(float)
df = df.drop(["cases", "deaths"], axis=1)

# add missing columns to match database
df["GeoArea_FIPS"] = "00000"
df["GeoArea_Name"] = "United States"
df["Economic_Measure_Code"] = "NYTI_NAT_COV04"
df["Economic_Measure_Name"] = "COVID-19 Confirmed Deaths"
df["Measure_Name"] = ""
df["Unit_of_Measure_Code"] = "Count"

# read population data and grab latest population data
df_population = pd.read_csv(
    "./Data/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt",
    sep="\t",
)
df_population = df_population[["Region Code", "2017"]]

filter1 = df_population["Region Code"] == "00000"
df_population = df_population[filter1]

df_population = df_population.rename(
    columns={"Region Code": "GeoArea_FIPS", "2017": "Population (2017)"}
)
df_population["Population (2017)"] = df_population["Population (2017)"] * 1000

df = df.merge(df_population)

# reset columns
columns = [
    "GeoArea_FIPS",
    "GeoArea_Name",
    "Economic_Measure_Code",
    "Economic_Measure_Name",
    "Measure_Name",
    "Data_Period_Business_Key",
    "Estimated_Value",
    "Unit_of_Measure_Code",
    "Population (2017)",
]
df = df[columns]
df.set_index("GeoArea_FIPS", inplace=True)

# save as txt
df.to_csv("./Updates/STG_NYTI_NAT_COVID_19_Deaths.txt", sep="\t")
