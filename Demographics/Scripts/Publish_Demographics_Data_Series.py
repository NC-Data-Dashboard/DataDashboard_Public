import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc


backup_df = pd.read_csv(
    "./Updates/STG_WNCD_Demographics_FRED_Data_Series.txt", sep="\t"
)
backup_df.to_csv("./Backups/STG_WNCD_Demographics_FRED_Series_BACKUP.txt", sep="\t")


#### FRED ####

# Civilian Labor Force
df1 = pd.read_csv(
    "./Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt", sep="\t"
)
df1["Economic_Measure_Code"] = "FRED_LFN_00000"
df1["Economic_Measure_Name"] = "Civilian Labor Force by County"
df1["Unit_of_Measure_Code"] = "PRS"
df1["Calculation_Type"] = "Level"
df1["Default_Scale"] = "0"

df1 = df1.rename(columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"})

df1 = df1.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
    ],
    var_name="Date",
    value_name="Estimated_Value",
)

df1["Estimation_Qualifier"] = ""


# EQFXSUBPRIME
df2 = pd.read_csv("./Updates/STG_FRED_EQFXSUBPRIME.txt", sep="\t")
df2["Economic_Measure_Code"] = "FRED_EQFXSUBPRIME0_00000"
df2["Economic_Measure_Name"] = "Total across all Population groups"
df2["Unit_of_Measure_Code"] = "PCT"
df2["Calculation_Type"] = "Percent"
df2["Default_Scale"] = "2"

df2 = df2.rename(columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"})

df2 = df2.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
    ],
    var_name="Date",
    value_name="Estimated_Value",
)

df2["Estimation_Qualifier"] = ""


# Education Rate
df3 = pd.read_csv(
    "./Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt",
    sep="\t",
)
df3["Economic_Measure_Code"] = "FRED_S1501ACSTOTAL0_00000"
df3[
    "Economic_Measure_Name"
] = "People 25 Years and Over Who Have Completed an Associates Degree or Higher (5-year estimate) by County (Percent)"
df3["Unit_of_Measure_Code"] = "PCS"
df3["Calculation_Type"] = "Percent"
df3["Default_Scale"] = "2"

df3 = df3.rename(columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"})

df3 = df3.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
    ],
    var_name="Date",
    value_name="Estimated_Value",
)

df3["Estimation_Qualifier"] = ""


# Population
df4 = pd.read_csv(
    "./Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt",
    sep="\t",
)
df4["Economic_Measure_Code"] = "FRED_POP_00000"
df4["Economic_Measure_Name"] = "Resident Population by County"
df4["Unit_of_Measure_Code"] = "PRS"
df4["Calculation_Type"] = "Level"
df4["Default_Scale"] = "-3"

df4 = df4.rename(columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"})

df4 = df4.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
    ],
    var_name="Date",
    value_name="Estimated_Value",
)

df4["Estimation_Qualifier"] = ""


df_list = [df2, df3, df4]
df_fred = df1.append(df_list)


column_list = df_fred.columns.values
for i in column_list:
    df_fred.loc[df_fred[i].isnull(), i] = 0

df_fred["PZ_Name"] = ""
df_fred["WDB_Name"] = ""
df_fred["Data_Period_Type"] = "YR"

df_fred["Date"] = pd.to_datetime(df_fred["Date"], errors="coerce")
df_fred["Data_Period_Business_Key"] = df_fred["Date"].dt.strftime("%Y")
df_fred["Data_Period_Name"] = df_fred["Date"].dt.strftime("The Year %Y")


#### Final Copy ####
# df = df_land.append(df_fred)
df = df_fred

# North Central
df.loc[
    df["GeoArea_Name"].str.contains(
        "Chatham|Durham|Edgecombe|Franklin|Granville|Harnett|Johnston|Lee|Nash|Orange|Person|Vance|Wake|Warren|Wilson"
    ),
    "PZ_Name",
] = "North Central"

# Northeast
df.loc[
    df["GeoArea_Name"].str.contains(
        "Beaufort|Bertie|Camden|Chowan|Tyrrell|Hyde|Currituck|Dare|Gates|Halifax|Hertford|Hyde|Martin|Northampton|Pasquotank|Perquimans|Pitt|Washington"
    ),
    "PZ_Name",
] = "Northeast"

# Northwest
df.loc[
    df["GeoArea_Name"].str.contains(
        "Avery|Alleghany|Alexander|Ashe|Burke|Caldwell|Catawba|McDowell|Mitchell|Watauga|Wilkes|Yancey"
    ),
    "PZ_Name",
] = "Northwest"

# Piedmont-Triad
df.loc[
    df["GeoArea_Name"].str.contains(
        "Alamance|Caswell|Davidson|Davie|Forsyth|Guilford|Randolph|Rockingham|Stokes|Surry|Yadkin"
    ),
    "PZ_Name",
] = "Piedmont-Triad"

# South Central
df.loc[
    df["GeoArea_Name"].str.contains(
        "Bladen|Columbus|Cumberland|Hoke|Montgomery|Moore|Richmond|Roberson|Sampson|Scotland"
    ),
    "PZ_Name",
] = "South Central"

# Southeast
df.loc[
    df["GeoArea_Name"].str.contains(
        "Jones|Brunswick|Carteret|Craven|Duplin|Greene|Lenoir|New Hanover|Onslow|Pamlico|Pender|Wayne"
    ),
    "PZ_Name",
] = "Southeast"

# Southwest
df.loc[
    df["GeoArea_Name"].str.contains(
        "Anson|Cabarrus|Cleveland|Gaston|Iredell|Lincoln|Mecklenburg|Rowan|Stanly|Union"
    ),
    "PZ_Name",
] = "Southwest"

# Western
df.loc[
    df["GeoArea_Name"].str.contains(
        "Graham|Buncombe|Cherokee|Clay|Haywood|Henderson|Jackson|Macon|Madison|Polk|Rutherford|Swain|Transylvania"
    ),
    "PZ_Name",
] = "Western"

# Cape Fear
df.loc[
    df["GeoArea_Name"].str.contains("Brunswick|Columbus|New Hanover|Pender"), "WDB_Name"
] = "Cape Fear"

# Capital Area
df.loc[df["GeoArea_Name"].str.contains("Johnson|Wake"), "WDB_Name"] = "Capital Area"

# Centralina
df.loc[
    df["GeoArea_Name"].str.contains(
        "Anson|Cabarrus|Iredell|Lincoln|Rowan|Stanley|Union"
    ),
    "WDB_Name",
] = "Centralina"

# Charlotte Works
df.loc[df["GeoArea_Name"].str.contains("Mecklenburgh"), "WDB_Name"] = "Charlotte Works"

# Cumberland County
df.loc[df["GeoArea_Name"].str.contains("Cumberland"), "WDB_Name"] = "Cumberland County"

# DavidsonWorks, Inc.
df.loc[df["GeoArea_Name"].str.contains("Davidson"), "WDB_Name"] = "DavidsonWorks, Inc."

# Durham
df.loc[df["GeoArea_Name"].str.contains("Durham"), "WDB_Name"] = "Durham"

# Eastern Carolina
df.loc[
    df["GeoArea_Name"].str.contains(
        "Carteret|Craven|Duplin|Greene|Jones|Lenoir|Onslow|Pamlico|Wayne"
    ),
    "WDB_Name",
] = "Eastern Carolina"

# Gaston County
df.loc[df["GeoArea_Name"].str.contains("Gaston"), "WDB_Name"] = "Gaston County"

# Greensboro/High Point/Guilford
df.loc[
    df["GeoArea_Name"].str.contains("Guilford"), "WDB_Name"
] = "Greensboro/High Point/Guilford"

# High Country
df.loc[
    df["GeoArea_Name"].str.contains(
        "Alleghany|Ashe|Yancey|Avery|Mitchell|Watauga|Wilkes"
    ),
    "WDB_Name",
] = "High Country"

# Kerr-Tar
df.loc[
    df["GeoArea_Name"].str.contains("Caswell|Franklin|Granville|Person|Vance|Warren"),
    "WDB_Name",
] = "Kerr-Tar"

# Lumber River
df.loc[
    df["GeoArea_Name"].str.contains("Bladen|Hoke|Richmond|Roberson|Scotland"),
    "WDB_Name",
] = "Lumber River"

# Mountain Area
df.loc[
    df["GeoArea_Name"].str.contains("Buncombe|Henderson|Madison|Transylvania"),
    "WDB_Name",
] = "Mountain Area"

# Northeastern
df.loc[
    df["GeoArea_Name"].str.contains(
        "Camden|Chowan|Currituck|Dare|Gates|Hyde|Pasquotank|Perquimans|Washington|Tyrrell"
    ),
    "WDB_Name",
] = "Northeastern"

# Northwest Piedmont
df.loc[
    df["GeoArea_Name"].str.contains("Davie|Forsyth|Rockingham|Stokes|Surry|Yadkin"),
    "WDB_Name",
] = "Northwest Piedmont"

# Region C
df.loc[
    df["GeoArea_Name"].str.contains("Cleveland|McDowell|Polk|Rutherford"), "WDB_Name"
] = "Region C"

# Region Q
df.loc[
    df["GeoArea_Name"].str.contains("Beaufort|Bertie|Hertford|Martin|Pitt"), "WDB_Name"
] = "Region Q"

# Regional Partnership
df.loc[
    df["GeoArea_Name"].str.contains("Alamance|Montgomery|Moore|Orange|Randolph"),
    "WDB_Name",
] = "Regional Partnership"

# Southwestern
df.loc[
    df["GeoArea_Name"].str.contains("Cherokee|Clay|Graham|Haywood|Macon|Jackson|Swain"),
    "WDB_Name",
] = "Southwestern"

# Triangle South
df.loc[
    df["GeoArea_Name"].str.contains("Chatham|Harnett|Sampson|Lee"), "WDB_Name"
] = "Triangle South"

# Turning Point
df.loc[
    df["GeoArea_Name"].str.contains("Edgecombe|Halifax|Nash|Northampton|Wilson"),
    "WDB_Name",
] = "Turning Point"

# Western Piedmont
df.loc[
    df["GeoArea_Name"].str.contains("Alexander|Burke|Caldwell|Catawba"), "WDB_Name"
] = "Western Piedmont"

df["Data_Period_Begin_Datetime"] = df["Date"].dt.strftime("%m/1/%Y %#H:%M")

columns = [
    "GeoArea_FIPS",
    "GeoArea_Name",
    "PZ_Name",
    "WDB_Name",
    "Economic_Measure_Code",
    "Economic_Measure_Name",
    "Data_Period_Business_Key",
    "Estimated_Value",
    "Estimation_Qualifier",
    "Unit_of_Measure_Code",
    "Default_Scale",
    "Data_Period_Type",
    "Data_Period_Name",
    "Data_Period_Begin_Datetime",
    "Calculation_Type",
]
df = df[columns]

# Add old Demographics data
df_old = pd.read_csv("./Data/old_Demographics.txt", sep="\t")

df = df_old.append(df)

df.set_index("GeoArea_FIPS", inplace=True)

df.to_csv("./Updates/STG_WNCD_Demographics_FRED_Data_Series.txt", sep="\t")

'''
df = df.reset_index()

# Connect to [database]
con = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=[server];"
    "Database=[database];"
    "Trusted_Connection=yes;",
    autocommit=True,
)

c = con.cursor()

c.execute("drop table STG_WNCD_Demographics_FRED_Data_Series_BACKUP")

c.execute(
    """sp_rename 'STG_WNCD_Demographics_FRED_Data_Series', 'STG_WNCD_Demographics_FRED_Data_Series_BACKUP';"""
)

params = urllib.parse.quote_plus(
    r"Driver={SQL Server};"
    r"Server=[server];"
    r"Database=[database];"
    r"Trusted_Connection=yes;"
)

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df.to_sql(
    "STG_WNCD_Demographics_FRED_Data_Series",
    con=engine,
    if_exists="replace",
    index=False,
)
'''
