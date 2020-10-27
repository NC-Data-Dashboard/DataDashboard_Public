print("Important!")
print("Make sure you update all dates to match the date of new data.")
print("Check line 14, 31, 32, 37, and 44!")

# Imports
import pandas as pd

# Create backups
df_backup = pd.read_csv("./Updates/STG_BEA_MSALESUSETAX_0002.txt")
df_backup.to_csv("./Backups/STG_BEA_MSALESUSETAX_0002_BACKUP.txt")

# Get new data from NCDOR (change month value in file name to get new month of data)
df = pd.read_excel(
    "https://files.nc.gov/ncdor/documents/reports/monthly_sales_10-20.xls", skiprows=10
)

# Drop NaN row
df = df.drop(df.index[0])
df = df[:-8]

# Remove columns with amiguous names
df = df.loc[:, ~df.columns.str.contains("Unnamed")]

# Create new dataframe with second set of counties
df1 = pd.DataFrame(df, columns=["County.1", "and Purchases*.1"])

# Drop second set of counties from original dataframe
df = df.drop(columns=["County.1", "and Purchases*.1", "Collections*", "Collections*.1"])

# Rename columns
df = df.rename(columns={"County.1": "County", "and Purchases*": "10-01-20"})
df1 = df1.rename(columns={"County.1": "County", "and Purchases*.1": "10-01-20"})

# Append dataframes
df_list = [df, df1]
df_main = df.append(df_list)
df_main["Date"] = "10/01/2020"

# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_main = df_main.dropna(how="all")
df_main = df_main.fillna("0")

# Change dtypes to Int
df_main["10-01-20"] = df_main["10-01-20"].astype(float)

# Drop junk rows
df_main = df_main[:-10]

# Add FIPS
df_fips = pd.read_csv("./FIPS_Codes.csv")
df_main = pd.merge(df_main, df_fips, on=["County", "County"])

df_main = df_main.drop(df_main.index[[0]])
df_main = df_main.drop(
    columns=[
        "Unnamed: 0",
        "RegionName",
        "State",
        "Metro",
        "StateCodeFIPS",
        "MunicipalCodeFIPS",
    ],
    axis=1,
)

df_main = df_main.drop_duplicates()
df_main = df_main.rename(columns={"County": "GeoArea_Name", "GeoFIPS": "GeoArea_FIPS"})
df_main["GeoArea_Name"] = df_main["GeoArea_Name"] + ", NC"

df_main = df_main.melt(
    id_vars=["GeoArea_FIPS", "GeoArea_Name"],
    var_name="Date",
    value_name="Published_Value",
)
df_main = df_main.dropna()
df_main["Published_Value"] = df_main["Published_Value"].astype(str)
df_main = df_main[~df_main["Published_Value"].str.contains("/")]
df_main = df_main.sort_values(by=["GeoArea_FIPS", "Date"], ascending=True)

# Get overall data
df_overall = pd.read_csv("./Updates/STG_BEA_MSALESUSETAX_0002.txt", sep="\t")

# Append new data to overall data
df_end = df_overall.append(df_main)

# Save new data source
df_end.to_csv("./Updates/STG_BEA_MSALESUSETAX_0002.txt", sep="\t")
