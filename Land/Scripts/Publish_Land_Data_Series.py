import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc


backup_df = pd.read_csv("./Updates/STG_WNCD_Land_Data_Series.txt", sep="\t")
backup_df.to_csv("./Backups/STG_WNCD_Land_Data_Series_BACKUP.txt", sep="\t")


#### ZLLW ####

# Median Sale Price
df1 = pd.read_csv("./Updates/STG_ZLLW_County_MedianSalePrice_AllHomes.txt", sep="\t")
df1["Economic_Measure_Code"] = "ZLLW_CNTY_MLP04"
df1["Economic_Measure_Name"] = "County Median Sale Price All Homes"

# Median Value Per Sqft
df2 = pd.read_csv("./Updates/STG_ZLLW_County_MedianValuePerSqft_AllHomes.txt", sep="\t")
df2["Economic_Measure_Code"] = "ZLLW_CNTY_MLP02"
df2["Economic_Measure_Name"] = "County Median Value Per Sqft All Homes"

# ZHVI
df3 = pd.read_csv("./Updates/STG_ZLLW_County_Zhvi_AllHomes.txt", sep="\t")
df3["Economic_Measure_Code"] = "ZLLW_CNTY_ZHVI0"
df3["Economic_Measure_Name"] = "Zillow House Value Index"

df_land = df1.append(df2)
df_land = df_land.append(df3)

df_land["StateCodeFIPS"] = df_land["StateCodeFIPS"].astype(str)
df_land["MunicipalCodeFIPS"] = df_land["MunicipalCodeFIPS"].astype(str)
df_land.loc[:, "MunicipalCodeFIPS"] = df_land["MunicipalCodeFIPS"].str.zfill(3)

df_land["GeoArea_FIPS"] = df_land["StateCodeFIPS"] + df_land["MunicipalCodeFIPS"]

df_land = df_land.drop(
    ["StateCodeFIPS", "State", "MunicipalCodeFIPS", "Metro", "SizeRank"], axis=1
)

df_land = df_land.rename(columns={"RegionName": "GeoArea_Name"})

df_land["GeoArea_Name"] = df_land["GeoArea_Name"].astype(str)

df_land = df_land.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
    ],
    var_name="Date",
    value_name="Published_Value",
)

df_land["PZ_Name"] = ""
df_land["WDB_Name"] = ""
df_land["Published_UOM"] = "INDEX"
df_land["Estimated_Real_Value"] = ""
df_land["Estimation_Qualifier"] = ""
df_land["Unit_Of_Measure_Code"] = ""
df_land["Default_Scale"] = "0"
df_land["Data_Period_Type"] = "MM"
df_land["Calculation_Type"] = "Index"

df_land["Date"] = pd.to_datetime(df_land["Date"], errors="coerce")
df_land["Data_Period_Business_Key"] = df_land["Date"].dt.strftime("%YM%m")
df_land["Data_Period_Name"] = df_land["Date"].dt.strftime("%B %Y")


#### FRED ####

# All Transactions
df4 = pd.read_csv(
    "./Updates/STG_FRED_All_Transactions_House_Price_Index_by_County.txt", sep="\t"
)
df4["Economic_Measure_Code"] = "FRED_ATNHPIUS_00000"
df4[
    "Economic_Measure_Name"
] = "All-Transactions House Price Index by County (Index 2000=100)"
df4["Published_UOM"] = "INDEX"
df4["Unit_of_Measure_Code"] = "INDEX"
df4["Calculation_Type"] = "Index"
df4["Default_Scale"] = "0"

df4 = df4.rename(columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"})

df4 = df4.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Published_UOM",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
    ],
    var_name="Date",
    value_name="Published_Value",
)

df4["Estimated_Real_Value"] = df4["Published_Value"]
df4["Estimation_Qualifier"] = df4["Published_Value"]


# New Private Housing
df5 = pd.read_csv(
    "./Updates/STG_FRED_New_Private_Housing_Structures_Authorized_by_Building_Permits_by_County.txt",
    sep="\t",
)
df5["Economic_Measure_Code"] = "FRED_BPPRIV0_00000"
df5[
    "Economic_Measure_Name"
] = "New Private Housing Structures Authorized by Building Permits by County (Units)"
df5["Published_UOM"] = "UNIT"
df5["Unit_of_Measure_Code"] = "UNIT"
df5["Calculation_Type"] = "Level"
df5["Default_Scale"] = "0"

df5 = df5.rename(columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"})

df5 = df5.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Published_UOM",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
    ],
    var_name="Date",
    value_name="Published_Value",
)

df5["Estimated_Real_Value"] = df5["Published_Value"]
df5["Estimation_Qualifier"] = df5["Published_Value"]


# Homeownership Rate
df6 = pd.read_csv("./Updates/STG_FRED_Homeownership_Rate_by_County.txt", sep="\t")
df6["Economic_Measure_Code"] = "FRED_HOWNRATEACS0_00000"
df6["Economic_Measure_Name"] = "Homeownership Rate by County (Rate)"
df6["Published_UOM"] = "RATE"
df6["Unit_of_Measure_Code"] = "RATE"
df6["Calculation_Type"] = "Percent"
df6["Default_Scale"] = "2"

df6 = df6.rename(columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"})

df6 = df6.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Published_UOM",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
    ],
    var_name="Date",
    value_name="Published_Value",
)

df6["Estimated_Real_Value"] = df6["Published_Value"]
df6["Estimation_Qualifier"] = ""

df_fred = df4.append(df5)
df_fred = df_fred.append(df6)

column_list = df_fred.columns.values
for i in column_list:
    df_fred.loc[df_fred[i].isnull(), i] = 0

df_fred["PZ_Name"] = ""
df_fred["WDB_Name"] = ""
df_fred["Data_Period_Type"] = "YY"

df_fred["Date"] = pd.to_datetime(df_fred["Date"], errors="coerce")
df_fred["Data_Period_Business_Key"] = df_fred["Date"].dt.strftime("%Y")
df_fred["Data_Period_Name"] = df_fred["Date"].dt.strftime("The Year %Y")


#### Final Copy ####
df = df_land.append(df_fred)

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
        "Bladen|Columbus|Cumberland|Hoke|Montgomery|Moore|Richmond|Robeson|Sampson|Scotland"
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
    "Published_Value",
    "Published_UOM",
    "Estimated_Real_Value",
    "Estimation_Qualifier",
    "Unit_Of_Measure_Code",
    "Default_Scale",
    "Data_Period_Type",
    "Data_Period_Name",
    "Data_Period_Begin_Datetime",
    "Calculation_Type",
]
df = df[columns]
df.set_index("GeoArea_FIPS", inplace=True)


df.to_csv("./Updates/STG_WNCD_Land_Data_Series.txt", sep="\t")
