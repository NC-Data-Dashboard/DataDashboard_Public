import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

# create backups
df_backup = pd.read_csv("./Updates/STG_NYTI_CNTY_COVID_19_Deaths.txt", sep="\t")
df_backup.to_csv("./Backups/STG_NYTI_CNTY_COVID_19_Deaths_BACKUP.txt", sep="\t")

# read data
df = pd.read_csv("./Data/covid-19-data/us-counties.csv")

# fitler to nc
filter1 = df["state"].str.contains("North Carolina")
df = df[filter1]

# clean
df["fips"] = df["fips"].astype(int)
df = df.rename(
    columns={
        "fips": "GeoArea_FIPS",
        "county": "GeoArea_Name",
        "deaths": "Estimated_Value",
        "date": "Data_Period_Business_Key",
    }
)
df = df.drop(["cases", "state"], axis=1)

df["Data_Period_Business_Key"] = pd.to_datetime(df["Data_Period_Business_Key"])
df["Estimated_Value"] = df["Estimated_Value"].astype(float)

# add missing columns to match database
df["Economic_Measure_Code"] = "NYTI_CNTY_COV02"
df["Economic_Measure_Name"] = "COVID-19 Confirmed Deaths"
df["Measure_Name"] = ""
df["Unit_of_Measure_Code"] = "Count"

# read population data and grab latest population data
df_population = pd.read_csv(
    "./Data/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt",
    sep="\t",
)
df_population = df_population[["Region Code", "2017"]]
df_population = df_population.rename(
    columns = {"Region Code": "GeoArea_FIPS", "2017": "Population (2017)"}
)
df_population["Population (2017)"] = df_population["Population (2017)"] * 1000


# copy data from population
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
df.to_csv("./Updates/STG_NYTI_CNTY_COVID_19_Deaths.txt", sep="\t")
