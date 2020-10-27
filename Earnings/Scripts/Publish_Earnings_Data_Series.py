import urllib
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import pyodbc
import datetime as dt


backup_df_master = pd.read_csv("./Updates/STG_WNCD_Earnings_Data_Series.txt", sep="\t")
backup_df_master.to_csv("./Backups/STG_WNCD_Earnings_Data_Series_BACKUP.txt", sep="\t")


#### BEA ####

df3 = pd.read_csv("./Updates/STG_BEA_CA5N_Personal_Income.txt", sep="\t")
df3["Economic_Measure_Code"] = "BEA_CA5N_0010"
df3["Published_UOM"] = "N$"
df3["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df3["Unit_of_Measure_Code"] = "R$"
df3["Default_Scale"] = "-3"
df3["Calculation_Type"] = "Level"
df3["Measure_Hierarchy_Level"] = "1"
df3["Data_Period_Type"] = "YR"


df1 = pd.read_csv("./Updates/STG_BEA_CA5N_Population.txt", sep="\t")
df1["Economic_Measure_Code"] = "BEA_CA5N_0020"
df1["Published_UOM"] = "UNIT"
df1["Estimation_Qualifier"] = ""
df1["Unit_of_Measure_Code"] = ""
df1["Default_Scale"] = "0"
df1["Calculation_Type"] = "Level"
df1["Measure_Hierarchy_Level"] = "1"
df1["Data_Period_Type"] = "YR"


df2 = pd.read_csv("./Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt", sep="\t")
df2["Economic_Measure_Code"] = "BEA_CA5N_0030"
df2["Published_UOM"] = "N$"
df2["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df2["Unit_of_Measure_Code"] = "R$"
df2["Default_Scale"] = "0"
df2["Calculation_Type"] = "Ratio"
df2["Measure_Hierarchy_Level"] = "1"
df2["Data_Period_Type"] = "YR"


df4 = pd.read_csv("./Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt", sep="\t")
df4["Economic_Measure_Code"] = "BEA_CA5N_0035"
df4["Published_UOM"] = "N$"
df4["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df4["Unit_of_Measure_Code"] = "R$"
df4["Default_Scale"] = "-3"
df4["Calculation_Type"] = "Level"
df4["Measure_Hierarchy_Level"] = "1"
df4["Data_Period_Type"] = "YR"


df5 = pd.read_csv("./Updates/STG_BEA_CA5N_Wages_and_Salaries.txt", sep="\t")
df5["Economic_Measure_Code"] = "BEA_CA5N_0050"
df5["Published_UOM"] = "N$"
df5["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df5["Unit_of_Measure_Code"] = "R$"
df5["Default_Scale"] = "-3"
df5["Calculation_Type"] = "Level"
df5["Measure_Hierarchy_Level"] = "2"
df5["Data_Period_Type"] = "YR"


df6 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt", sep="\t"
)
df6["Economic_Measure_Code"] = "BEA_CA5N_0060"
df6["Published_UOM"] = "N$"
df6["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df6["Unit_of_Measure_Code"] = "R$"
df6["Default_Scale"] = "-3"
df6["Calculation_Type"] = "Level"
df6["Measure_Hierarchy_Level"] = "2"
df6["Data_Period_Type"] = "YR"


df7 = pd.read_csv("./Updates/STG_BEA_CA5N_Proprietors_Income.txt", sep="\t")
df7["Economic_Measure_Code"] = "BEA_CA5N_0070"
df7["Published_UOM"] = "N$"
df7["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df7["Unit_of_Measure_Code"] = "R$"
df7["Default_Scale"] = "-3"
df7["Calculation_Type"] = "Level"
df7["Data_Period_Type"] = "YR"
df7["Measure_Hierarchy_Level"] = "2"


df8 = pd.read_csv("./Updates/STG_BEA_CA5N_Farm_Compensation.txt", sep="\t")
df8["Economic_Measure_Code"] = "BEA_CA5N_0081"
df8["Published_UOM"] = "N$"
df8["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df8["Unit_of_Measure_Code"] = "R$"
df8["Default_Scale"] = "-3"
df8["Calculation_Type"] = "Level"
df8["Measure_Hierarchy_Level"] = "2"
df8["Data_Period_Type"] = "YR"


df9 = pd.read_csv("./Updates/STG_BEA_CA5N_Private_Nonfarm_Compensation.txt", sep="\t")
df9["Economic_Measure_Code"] = "BEA_CA5N_0082"
df9["Published_UOM"] = "N$"
df9["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df9["Unit_of_Measure_Code"] = "R$"
df9["Default_Scale"] = "-3"
df9["Calculation_Type"] = "Level"
df9["Measure_Hierarchy_Level"] = "2"
df9["Data_Period_Type"] = "YR"


df10 = pd.read_csv("./Updates/STG_BEA_CA5N_Private_Nonfarm_Compensation.txt", sep="\t")
df10["Economic_Measure_Code"] = "BEA_CA5N_0090"
df10["Published_UOM"] = "N$"
df10["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df10["Unit_of_Measure_Code"] = "R$"
df10["Default_Scale"] = "-3"
df10["Calculation_Type"] = "Level"
df10["Measure_Hierarchy_Level"] = "3"
df10["Data_Period_Type"] = "YR"


df11 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt", sep="\t"
)
df11["Economic_Measure_Code"] = "BEA_CA5N_0100"
df11["Published_UOM"] = "N$"
df11["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df11["Unit_of_Measure_Code"] = "R$"
df11["Default_Scale"] = "-3"
df11["Calculation_Type"] = "Level"
df11["Measure_Hierarchy_Level"] = "4"
df11["Data_Period_Type"] = "YR"


df12 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt", sep="\t"
)
df12["Economic_Measure_Code"] = "BEA_CA5N_0200  "
df12["Published_UOM"] = "N$"
df12["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df12["Unit_of_Measure_Code"] = "R$"
df12["Default_Scale"] = "-3"
df12["Calculation_Type"] = "Level"
df12["Measure_Hierarchy_Level"] = "4"


df13 = pd.read_csv("./Updates/STG_BEA_CA5N_Utilities.txt", sep="\t")
df13["Economic_Measure_Code"] = "BEA_CA5N_0300"
df13["Published_UOM"] = "N$"
df13["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df13["Unit_of_Measure_Code"] = "R$"
df13["Default_Scale"] = "-3"
df13["Calculation_Type"] = "Level"
df13["Measure_Hierarchy_Level"] = "4"
df13["Data_Period_Type"] = "YR"


df14 = pd.read_csv("./Updates/STG_BEA_CA5N_Construction.txt", sep="\t")
df14["Economic_Measure_Code"] = "BEA_CA5N_0400"
df14["Published_UOM"] = "N$"
df14["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df14["Unit_of_Measure_Code"] = "R$"
df14["Default_Scale"] = "-3"
df14["Calculation_Type"] = "Level"
df14["Measure_Hierarchy_Level"] = "4"
df14["Data_Period_Type"] = "YR"


df15 = pd.read_csv("./Updates/STG_BEA_CA5N_Manufacturing.txt", sep="\t")
df15["Economic_Measure_Code"] = "BEA_CA5N_0500"
df15["Published_UOM"] = "N$"
df15["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df15["Unit_of_Measure_Code"] = "R$"
df15["Default_Scale"] = "-3"
df15["Calculation_Type"] = "Level"
df15["Measure_Hierarchy_Level"] = "4"
df15["Data_Period_Type"] = "YR"


df16 = pd.read_csv("./Updates/STG_BEA_CA5N_Wholesale_Trade.txt", sep="\t")
df16["Economic_Measure_Code"] = "BEA_CA5N_0600"
df16["Published_UOM"] = "N$"
df16["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df16["Unit_of_Measure_Code"] = "R$"
df16["Default_Scale"] = "-3"
df16["Calculation_Type"] = "Level"
df16["Measure_Hierarchy_Level"] = "4"
df16["Data_Period_Type"] = "YR"


df17 = pd.read_csv("./Updates/STG_BEA_CA5N_Retail_Trade.txt", sep="\t")
df17["Economic_Measure_Code"] = "BEA_CA5N_0700"
df17["Published_UOM"] = "N$"
df17["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df17["Unit_of_Measure_Code"] = "R$"
df17["Default_Scale"] = "-3"
df17["Calculation_Type"] = "Level"
df17["Measure_Hierarchy_Level"] = "4"
df17["Data_Period_Type"] = "YR"


df18 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Transportation_and_Warehousing.txt", sep="\t"
)
df18["Economic_Measure_Code"] = "BEA_CA5N_0800"
df18["Published_UOM"] = "N$"
df18["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df18["Unit_of_Measure_Code"] = "R$"
df18["Default_Scale"] = "-3"
df18["Calculation_Type"] = "Level"
df18["Measure_Hierarchy_Level"] = "4"
df18["Data_Period_Type"] = "YR"


df19 = pd.read_csv("./Updates/STG_BEA_CA5N_Information.txt", sep="\t")
df19["Economic_Measure_Code"] = "BEA_CA5N_0900"
df19["Published_UOM"] = "N$"
df19["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df19["Unit_of_Measure_Code"] = "R$"
df19["Default_Scale"] = "-3"
df19["Calculation_Type"] = "Level"
df19["Measure_Hierarchy_Level"] = "4"
df19["Data_Period_Type"] = "YR"


df20 = pd.read_csv("./Updates/STG_BEA_CA5N_Finance_and_Insurance.txt", sep="\t")
df20["Economic_Measure_Code"] = "BEA_CA5N_1000"
df20["Published_UOM"] = "N$"
df20["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df20["Unit_of_Measure_Code"] = "R$"
df20["Default_Scale"] = "-3"
df20["Calculation_Type"] = "Level"
df20["Measure_Hierarchy_Level"] = "4"
df20["Data_Period_Type"] = "YR"


df21 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt", sep="\t"
)
df21["Economic_Measure_Code"] = "BEA_CA5N_1100"
df21["Published_UOM"] = "N$"
df21["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df21["Unit_of_Measure_Code"] = "R$"
df21["Default_Scale"] = "-3"
df21["Calculation_Type"] = "Level"
df21["Measure_Hierarchy_Level"] = "4"
df21["Data_Period_Type"] = "YR"


df22 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt",
    sep="\t",
)
df22["Economic_Measure_Code"] = "BEA_CA5N_1200"
df22["Published_UOM"] = "N$"
df22["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df22["Unit_of_Measure_Code"] = "R$"
df22["Default_Scale"] = "-3"
df22["Calculation_Type"] = "Level"
df22["Measure_Hierarchy_Level"] = "4"
df22["Data_Period_Type"] = "YR"


df23 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt", sep="\t"
)
df23["Economic_Measure_Code"] = "BEA_CA5N_1300"
df23["Published_UOM"] = "N$"
df23["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df23["Unit_of_Measure_Code"] = "R$"
df23["Default_Scale"] = "-3"
df23["Calculation_Type"] = "Level"
df23["Measure_Hierarchy_Level"] = "4"
df23["Data_Period_Type"] = "YR"


df24 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt",
    sep="\t",
)
df24["Economic_Measure_Code"] = "BEA_CA5N_1400"
df24["Published_UOM"] = "N$"
df24["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df24["Unit_of_Measure_Code"] = "R$"
df24["Default_Scale"] = "-3"
df24["Calculation_Type"] = "Level"
df24["Measure_Hierarchy_Level"] = "4"
df24["Data_Period_Type"] = "YR"


df25 = pd.read_csv("./Updates/STG_BEA_CA5N_Educational_Services.txt", sep="\t")
df25["Economic_Measure_Code"] = "BEA_CA5N_1500"
df25["Published_UOM"] = "N$"
df25["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df25["Unit_of_Measure_Code"] = "R$"
df25["Default_Scale"] = "-3"
df25["Calculation_Type"] = "Level"
df25["Measure_Hierarchy_Level"] = "4"
df25["Data_Period_Type"] = "YR"


df26 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt", sep="\t"
)
df26["Economic_Measure_Code"] = "BEA_CA5N_1600"
df26["Published_UOM"] = "N$"
df26["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df26["Unit_of_Measure_Code"] = "R$"
df26["Default_Scale"] = "-3"
df26["Calculation_Type"] = "Level"
df26["Measure_Hierarchy_Level"] = "4"
df26["Data_Period_Type"] = "YR"


df27 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt", sep="\t"
)
df27["Economic_Measure_Code"] = "BEA_CA5N_1700"
df27["Published_UOM"] = "N$"
df27["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df27["Unit_of_Measure_Code"] = "R$"
df27["Default_Scale"] = "-3"
df27["Calculation_Type"] = "Level"
df27["Measure_Hierarchy_Level"] = "4"
df27["Data_Period_Type"] = "YR"


df28 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Accommodation_and_Food_Services.txt", sep="\t"
)
df28["Economic_Measure_Code"] = "BEA_CA5N_1800"
df28["Published_UOM"] = "N$"
df28["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df28["Unit_of_Measure_Code"] = "R$"
df28["Default_Scale"] = "-3"
df28["Calculation_Type"] = "Level"
df28["Measure_Hierarchy_Level"] = "4"
df28["Data_Period_Type"] = "YR"


df29 = pd.read_csv("./Updates/STG_BEA_CA5N_Other_Services.txt", sep="\t")
df29["Economic_Measure_Code"] = "BEA_CA5N_1900"
df29["Published_UOM"] = "N$"
df29["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df29["Unit_of_Measure_Code"] = "R$"
df29["Default_Scale"] = "-3"
df29["Calculation_Type"] = "Level"
df29["Measure_Hierarchy_Level"] = "4"
df29["Data_Period_Type"] = "YR"


df30 = pd.read_csv(
    "./Updates/STG_BEA_CA5N_Government_and_Government_Enterprises.txt", sep="\t"
)
df30["Economic_Measure_Code"] = "BEA_CA5N_2000"
df30["Published_UOM"] = "N$"
df30["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df30["Unit_of_Measure_Code"] = "R$"
df30["Default_Scale"] = "-3"
df30["Calculation_Type"] = "Level"
df30["Measure_Hierarchy_Level"] = "2"
df30["Data_Period_Type"] = "YR"


df31 = pd.read_csv("./Updates/STG_BEA_CA5N_Federal_Civilian_Government.txt", sep="\t")
df31["Economic_Measure_Code"] = "BEA_CA5N_2001"
df31["Published_UOM"] = "N$"
df31["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df31["Unit_of_Measure_Code"] = "R$"
df31["Default_Scale"] = "-3"
df31["Calculation_Type"] = "Level"
df31["Measure_Hierarchy_Level"] = "4"
df31["Data_Period_Type"] = "YR"


df32 = pd.read_csv("./Updates/STG_BEA_CA5N_Military_Government.txt", sep="\t")
df32["Economic_Measure_Code"] = "BEA_CA5N_2002"
df32["Published_UOM"] = "N$"
df32["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df32["Unit_of_Measure_Code"] = "R$"
df32["Default_Scale"] = "-3"
df32["Calculation_Type"] = "Level"
df32["Measure_Hierarchy_Level"] = "4"
df32["Data_Period_Type"] = "YR"


df33 = pd.read_csv("./Updates/STG_BEA_CA5N_State_Local_Government.txt", sep="\t")
df33["Economic_Measure_Code"] = "BEA_CA5N_2010"
df33["Published_UOM"] = "N$"
df33["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df33["Unit_of_Measure_Code"] = "R$"
df33["Default_Scale"] = "-3"
df33["Calculation_Type"] = "Level"
df33["Measure_Hierarchy_Level"] = "4"
df33["Data_Period_Type"] = "YR"


df34 = pd.read_csv("./Updates/STG_BEA_CA5N_State_Government.txt", sep="\t")
df34["Economic_Measure_Code"] = "BEA_CA5N_2011"
df34["Published_UOM"] = "N$"
df34["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df34["Unit_of_Measure_Code"] = "R$"
df34["Default_Scale"] = "-3"
df34["Calculation_Type"] = "Level"
df34["Measure_Hierarchy_Level"] = "5"
df34["Data_Period_Type"] = "YR"

df35 = pd.read_csv("./Updates/STG_BEA_CA5N_Local_Government.txt", sep="\t")
df35["Economic_Measure_Code"] = "BEA_CA5N_2012"
df35["Published_UOM"] = "N$"
df35["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df35["Unit_of_Measure_Code"] = "R$"
df35["Default_Scale"] = "-3"
df35["Calculation_Type"] = "Level"
df35["Measure_Hierarchy_Level"] = "5"
df35["Data_Period_Type"] = "YR"


df36 = pd.read_csv("./Updates/STG_BEA_MSALESUSETAX_0001.txt", sep="\t")
df36["Economic_Measure_Code"] = "NCDOR_MSALESUSETAX_0001"
df36["Economic_Measure_Name"] = "Monthly Sales and Use Tax Collections"
df36["Published_UOM"] = "N$"
# df36['Estimated_Real_Value'] = ''  #unknown calculation
df36["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df36["Unit_of_Measure_Code"] = "R$"
df36["Default_Scale"] = "0"
df36["Data_Period_Type"] = "MM"
df36["Calculation_Type"] = "Level"
df36["Measure_Hierarchy_Level"] = "1"


df37 = pd.read_csv("./Updates/STG_BEA_MSALESUSETAX_0002.txt", sep="\t")
df37["Economic_Measure_Code"] = "NCDOR_MSALESUSETAX_0002"
df37[
    "Economic_Measure_Name"
] = "Monthly Retail Sales on which Sales and Use Tax Collection are based"
df37["Published_UOM"] = "N$"
# df37['Estimated_Real_Value'] = ''  #unknown calculation
df37["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df37["Unit_of_Measure_Code"] = "R$"
df37["Default_Scale"] = "0"
df37["Data_Period_Type"] = "MM"
df37["Calculation_Type"] = "Level"
df37["Measure_Hierarchy_Level"] = "1"


df_list = [
    df2,
    df3,
    df4,
    df5,
    df6,
    df7,
    df8,
    df9,
    df10,
    df11,
    df12,
    df13,
    df14,
    df15,
    df16,
    df17,
    df18,
    df19,
    df20,
    df21,
    df22,
    df23,
    df24,
    df25,
    df26,
    df27,
    df28,
    df29,
    df30,
    df31,
    df32,
    df33,
    df34,
    df35,
]

df = df1.append(df_list)
df = df.rename(
    columns={
        "Description": "Economic_Measure_Name",
        "GeoFIPS": "GeoArea_FIPS",
        "GeoName": "GeoArea_Name",
    }
)
df = df.drop(
    ["Region", "TableName", "LineCode", "IndustryClassification", "Unit"], axis=1
)


df = df.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Published_UOM",
        "Unit_of_Measure_Code",
        "Data_Period_Type",
        "Calculation_Type",
        "Default_Scale",
        "Estimation_Qualifier",
        "Measure_Hierarchy_Level",
    ],
    var_name="Date",
    value_name="Published_Value",
)
# df_master['Published_Value'] = df_master['Published_Value'].astype(str)

df_master_list = [df36, df37]
df_master = df.append(df_master_list)


df_master["Estimated_Real_Value"] = ""  # unknown calculation
df_master["PZ_Name"] = ""
df_master["WDB_Name"] = ""
df_master["State_FIPS"] = "37000"
df_master["State_Name"] = "North Carolina"
df_master["Region_FIPS"] = "95000"
df_master["Region_Name"] = "Southeast"
df_master["Nation_Name"] = "USA"
df_master["Nation_FIPS"] = "00000"
df_master["GeoFIPS_Type"] = "CNTY"


df_master["Date"] = pd.to_datetime(df_master["Date"], errors="coerce")
df_master["Data_Period_Business_Key"] = df_master["Date"].dt.strftime("%Y")
df_master["Data_Period_Name"] = df_master["Date"].dt.strftime("The Year %Y")
df_master["Data_Period_Begin_Datetime"] = df_master["Date"].dt.strftime(
    "%m/1/%Y %#H:%M"
)
df_master = df_master.drop(["Date"], axis=1)

# North Central
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Chatham|Durham|Edgecombe|Franklin|Granville|Harnett|Johnston|Lee|Nash|Orange|Person|Vance|Wake|Warren|Wilson",
        na=False,
    ),
    "PZ_Name",
] = "North Central"

# Northeast
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Beaufort|Bertie|Camden|Chowan|Tyrrell|Hyde|Currituck|Dare|Gates|Halifax|Hertford|Hyde|Martin|Northampton|Pasquotank|Perquimans|Pitt|Washington",
        na=False,
    ),
    "PZ_Name",
] = "Northeast"

# Northwest
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Avery|Alleghany|Alexander|Ashe|Burke|Caldwell|Catawba|McDowell|Mitchell|Watauga|Wilkes|Yancey",
        na=False,
    ),
    "PZ_Name",
] = "Northwest"

# Piedmont-Triad
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Alamance|Caswell|Davidson|Davie|Forsyth|Guilford|Randolph|Rockingham|Stokes|Surry|Yadkin",
        na=False,
    ),
    "PZ_Name",
] = "Piedmont-Triad"

# South Central
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Bladen|Columbus|Cumberland|Hoke|Montgomery|Moore|Richmond|Roberson|Sampson|Scotland",
        na=False,
    ),
    "PZ_Name",
] = "South Central"

# Southeast
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Jones|Brunswick|Carteret|Craven|Duplin|Greene|Lenoir|New Hanover|Onslow|Pamlico|Pender|Wayne",
        na=False,
    ),
    "PZ_Name",
] = "Southeast"

# Southwest
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Anson|Cabarrus|Cleveland|Gaston|Iredell|Lincoln|Mecklenburg|Rowan|Stanly|Union",
        na=False,
    ),
    "PZ_Name",
] = "Southwest"

# Western
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Graham|Buncombe|Cherokee|Clay|Haywood|Henderson|Jackson|Macon|Madison|Polk|Rutherford|Swain|Transylvania",
        na=False,
    ),
    "PZ_Name",
] = "Western"

# Cape Fear
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Brunswick|Columbus|New Hanover|Pender", na=False
    ),
    "WDB_Name",
] = "Cape Fear"

# Capital Area
df_master.loc[
    df_master["GeoArea_Name"].str.contains("Johnson|Wake", na=False), "WDB_Name"
] = "Capital Area"

# Centralina
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Anson|Cabarrus|Iredell|Lincoln|Rowan|Stanley|Union", na=False
    ),
    "WDB_Name",
] = "Centralina"

# Charlotte Works
df_master.loc[
    df_master["GeoArea_Name"].str.contains("Mecklenburgh", na=False), "WDB_Name"
] = "Charlotte Works"

# Cumberland County
df_master.loc[
    df_master["GeoArea_Name"].str.contains("Cumberland", na=False), "WDB_Name"
] = "Cumberland County"

# DavidsonWorks, Inc.
df_master.loc[
    df_master["GeoArea_Name"].str.contains("Davidson", na=False), "WDB_Name"
] = "DavidsonWorks, Inc."

# Durham
df_master.loc[
    df_master["GeoArea_Name"].str.contains("Durham", na=False), "WDB_Name"
] = "Durham"

# Eastern Carolina
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Carteret|Craven|Duplin|Greene|Jones|Lenoir|Onslow|Pamlico|Wayne", na=False
    ),
    "WDB_Name",
] = "Eastern Carolina"

# Gaston County
df_master.loc[
    df_master["GeoArea_Name"].str.contains("Gaston", na=False), "WDB_Name"
] = "Gaston County"

# Greensboro/High Point/Guilford
df_master.loc[
    df_master["GeoArea_Name"].str.contains("Guilford", na=False), "WDB_Name"
] = "Greensboro/High Point/Guilford"

# High Country
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Alleghany|Ashe|Yancey|Avery|Mitchell|Watauga|Wilkes", na=False
    ),
    "WDB_Name",
] = "High Country"

# Kerr-Tar
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Caswell|Franklin|Granville|Person|Vance|Warren", na=False
    ),
    "WDB_Name",
] = "Kerr-Tar"

# Lumber River
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Bladen|Hoke|Richmond|Roberson|Scotland", na=False
    ),
    "WDB_Name",
] = "Lumber River"

# Mountain Area
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Buncombe|Henderson|Madison|Transylvania", na=False
    ),
    "WDB_Name",
] = "Mountain Area"

# Northeastern
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Camden|Chowan|Currituck|Dare|Gates|Hyde|Pasquotank|Perquimans|Washington|Tyrrell",
        na=False,
    ),
    "WDB_Name",
] = "Northeastern"

# Northwest Piedmont
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Davie|Forsyth|Rockingham|Stokes|Surry|Yadkin", na=False
    ),
    "WDB_Name",
] = "Northwest Piedmont"

# Region C
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Cleveland|McDowell|Polk|Rutherford", na=False
    ),
    "WDB_Name",
] = "Region C"

# Region Q
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Beaufort|Bertie|Hertford|Martin|Pitt", na=False
    ),
    "WDB_Name",
] = "Region Q"

# Regional Partnership
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Alamance|Montgomery|Moore|Orange|Randolph", na=False
    ),
    "WDB_Name",
] = "Regional Partnership"

# Southwestern
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Cherokee|Clay|Graham|Haywood|Macon|Jackson|Swain", na=False
    ),
    "WDB_Name",
] = "Southwestern"

# Triangle South
df_master.loc[
    df_master["GeoArea_Name"].str.contains("Chatham|Harnett|Sampson|Lee", na=False),
    "WDB_Name",
] = "Triangle South"

# Turning Point
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Edgecombe|Halifax|Nash|Northampton|Wilson", na=False
    ),
    "WDB_Name",
] = "Turning Point"

# Western Piedmont
df_master.loc[
    df_master["GeoArea_Name"].str.contains(
        "Alexander|Burke|Caldwell|Catawba", na=False
    ),
    "WDB_Name",
] = "Western Piedmont"


columns = [
    "GeoArea_FIPS",
    "GeoArea_Name",
    "Economic_Measure_Code",
    "Economic_Measure_Name",
    "Data_Period_Business_Key",
    "Published_Value",
    "Published_UOM",
    "Estimated_Real_Value",
    "Estimation_Qualifier",
    "Unit_of_Measure_Code",
    "Default_Scale",
    "Data_Period_Type",
    "Data_Period_Name",
    "Data_Period_Begin_Datetime",
    "Calculation_Type",
    "Measure_Hierarchy_Level",
    "GeoFIPS_Type",
    "County_FIPS",
    "County_Name",
    "PZ_Name",
    "WDB_Name",
    "State_FIPS",
    "State_Name",
    "Region_FIPS",
    "Region_Name",
    "Nation_FIPS",
    "Nation_Name",
]

df_master.to_csv("./Updates/STG_WNCD_Earnings_Data_Series.txt", sep="\t")

# df_master = df_master.reset_index()
