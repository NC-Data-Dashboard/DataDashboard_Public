import urllib
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc


backup_df = pd.read_csv("./Updates/STG_WNCD_Labor_Data_Series.txt", sep="\t")
backup_df.to_csv("./Backups/STG_WNCD_Labor_Data_Series_BACKUP.txt", sep="\t")


#### CA5N ####

# personal income
df1 = pd.read_csv("./Updates/STG_BEA_CA5N_Personal_Income.txt", sep="\t")
df1["Economic_Measure_Code"] = "BEA_CA5N_0010"
df1["Published_UOM"] = "N$"
df1["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df1["Unit_of_Measure_Code"] = "R$"
df1["Default_Scale"] = "-3"
df1["Calculation_Type"] = "Level"

# population
df2 = pd.read_csv("./Updates/STG_BEA_CA5N_Population.txt", sep="\t")
df2["Economic_Measure_Code"] = "BEA_CA5N_0020"
df2["Published_UOM"] = "UNIT"
df2["Estimation_Qualifier"] = ""
df2["Unit_of_Measure_Code"] = "UNIT"
df2["Default_Scale"] = "0"
df2["Calculation_Type"] = "Level"

# per capita personal income
df3 = pd.read_csv("./Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt", sep="\t")
df3["Economic_Measure_Code"] = "BEA_CA5N_0030"
df3["Published_UOM"] = "N$"
df3["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df3["Unit_of_Measure_Code"] = "R$"
df3["Default_Scale"] = "0"
df3["Calculation_Type"] = "Ratio"

# Earnings by place of work
df4 = pd.read_csv("./Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt", sep="\t")
df4["Economic_Measure_Code"] = "BEA_CA5N_0035"
df4["Published_UOM"] = "N$"
df4["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df4["Unit_of_Measure_Code"] = "R$"
df4["Default_Scale"] = "-3"
df4["Calculation_Type"] = "Level"

# Average Compensation
df5 = pd.read_csv("./Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt", sep="\t")
df5["Economic_Measure_Code"] = "BEA_CA6N_0009"
df5["Published_UOM"] = "N$"
df5["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df5["Unit_of_Measure_Code"] = "R$"
df5["Default_Scale"] = "0"
df5["Calculation_Type"] = "Ratio"


#### CA6N ####

# Compensation of Employees
df6 = pd.read_csv("./Updates/STG_BEA_CA6N_Compensation_of_Employees.txt", sep="\t")
df6["Economic_Measure_Code"] = "BEA_CA6N_0001"
df6["Published_UOM"] = "N$"
df6["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df6["Unit_of_Measure_Code"] = "R$"
df6["Default_Scale"] = "-3"
df6["Calculation_Type"] = "Level"

# Wages and Salaries
df7 = pd.read_csv("./Updates/STG_BEA_CA6N_Wages_and_Salaries.txt", sep="\t")
df7["Economic_Measure_Code"] = "BEA_CA6N_0005"
df7["Published_UOM"] = "N$"
df7["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df7["Unit_of_Measure_Code"] = "R$"
df7["Default_Scale"] = "-3"
df7["Calculation_Type"] = "Level"

# Supplements to wages and salaries
df8 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Supplements_to_Wages_and_Salaries.txt", sep="\t"
)
df8["Economic_Measure_Code"] = "BEA_CA6N_0006"
df8["Published_UOM"] = "N$"
df8["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df8["Unit_of_Measure_Code"] = "R$"
df8["Default_Scale"] = "-3"
df8["Calculation_Type"] = "Level"

# Employer Contributions for Employee Pension
df9 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds.txt",
    sep="\t",
)
df9["Economic_Measure_Code"] = "BEA_CA6N_0007"
df9["Published_UOM"] = "N$"
df9["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df9["Unit_of_Measure_Code"] = "R$"
df9["Default_Scale"] = "-3"
df9["Calculation_Type"] = "Level"

# Employer Contributions for Government
df10 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance.txt",
    sep="\t",
)
df10["Economic_Measure_Code"] = "BEA_CA6N_0008"
df10["Published_UOM"] = "N$"
df10["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df10["Unit_of_Measure_Code"] = "R$"
df10["Default_Scale"] = "-3"
df10["Calculation_Type"] = "Level"

# Farm Compensation
df11 = pd.read_csv("./Updates/STG_BEA_CA6N_Farm_Compensation.txt", sep="\t")
df11["Economic_Measure_Code"] = "BEA_CA6N_0081"
df11["Published_UOM"] = "N$"
df11["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df11["Unit_of_Measure_Code"] = "R$"
df11["Default_Scale"] = "-3"
df11["Calculation_Type"] = "Level"

# NonFarm Compensation
df12 = pd.read_csv("./Updates/STG_BEA_CA6N_Nonfarm_Compensation.txt", sep="\t")
df12["Economic_Measure_Code"] = "BEA_CA6N_0082"
df12["Published_UOM"] = "N$"
df12["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df12["Unit_of_Measure_Code"] = "R$"
df12["Default_Scale"] = "-3"
df12["Calculation_Type"] = "Level"

# Private Nonfarm
df13 = pd.read_csv("./Updates/STG_BEA_CA6N_Private_Nonfarm_Compensation.txt", sep="\t")
df13["Economic_Measure_Code"] = "BEA_CA6N_0090"
df13["Published_UOM"] = "N$"
df13["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df13["Unit_of_Measure_Code"] = "R$"
df13["Default_Scale"] = "-3"
df13["Calculation_Type"] = "Level"

# Average Compensation
df14 = pd.read_csv("./Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt", sep="\t")
df14["Economic_Measure_Code"] = "BEA_CA6N_0100"
df14["Published_UOM"] = "N$"
df14["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df14["Unit_of_Measure_Code"] = "R$"
df14["Default_Scale"] = "-3"
df14["Calculation_Type"] = "Level"

# Mining
df15 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt", sep="\t"
)
df15["Economic_Measure_Code"] = "BEA_CA6N_0200"
df15["Published_UOM"] = "N$"
df15["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df15["Unit_of_Measure_Code"] = "R$"
df15["Default_Scale"] = "-3"
df15["Calculation_Type"] = "Level"

# Utilities
df16 = pd.read_csv("./Updates/STG_BEA_CA6N_Utilities.txt", sep="\t")
df16["Economic_Measure_Code"] = "BEA_CA6N_0300"
df16["Published_UOM"] = "N$"
df16["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df16["Unit_of_Measure_Code"] = "R$"
df16["Default_Scale"] = "-3"
df16["Calculation_Type"] = "Level"

# Construction
df17 = pd.read_csv("./Updates/STG_BEA_CA6N_Construction.txt", sep="\t")
df17["Economic_Measure_Code"] = "BEA_CA6N_0400"
df17["Published_UOM"] = "N$"
df17["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df17["Unit_of_Measure_Code"] = "R$"
df17["Default_Scale"] = "-3"
df17["Calculation_Type"] = "Level"

# Manufacturing
df18 = pd.read_csv("./Updates/STG_BEA_CA6N_Manufacturing.txt", sep="\t")
df18["Economic_Measure_Code"] = "BEA_CA6N_0500"
df18["Published_UOM"] = "N$"
df18["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df18["Unit_of_Measure_Code"] = "R$"
df18["Default_Scale"] = "-3"
df18["Calculation_Type"] = "Level"

# Wholesale Trade
df19 = pd.read_csv("./Updates/STG_BEA_CA6N_Wholesale_Trade.txt", sep="\t")
df19["Economic_Measure_Code"] = "BEA_CA6N_0600"
df19["Published_UOM"] = "N$"
df19["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df19["Unit_of_Measure_Code"] = "R$"
df19["Default_Scale"] = "-3"
df19["Calculation_Type"] = "Level"

# Retail Trade
df20 = pd.read_csv("./Updates/STG_BEA_CA6N_Retail_Trade.txt", sep="\t")
df20["Economic_Measure_Code"] = "BEA_CA6N_0700"
df20["Published_UOM"] = "N$"
df20["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df20["Unit_of_Measure_Code"] = "R$"
df20["Default_Scale"] = "-3"
df20["Calculation_Type"] = "Level"

# Transportation
df21 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Transportation_and_Warehousing.txt", sep="\t"
)
df21["Economic_Measure_Code"] = "BEA_CA6N_0800"
df21["Published_UOM"] = "N$"
df21["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df21["Unit_of_Measure_Code"] = "R$"
df21["Default_Scale"] = "-3"
df21["Calculation_Type"] = "Level"

# Information
df22 = pd.read_csv("./Updates/STG_BEA_CA6N_Information.txt", sep="\t")
df22["Economic_Measure_Code"] = "BEA_CA6N_0900"
df22["Published_UOM"] = "N$"
df22["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df22["Unit_of_Measure_Code"] = "R$"
df22["Default_Scale"] = "-3"
df22["Calculation_Type"] = "Level"

# Finance and Insurance
df23 = pd.read_csv("./Updates/STG_BEA_CA6N_Finance_and_Insurance.txt", sep="\t")
df23["Economic_Measure_Code"] = "BEA_CA6N_1000"
df23["Published_UOM"] = "N$"
df23["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df23["Unit_of_Measure_Code"] = "R$"
df23["Default_Scale"] = "-3"
df23["Calculation_Type"] = "Level"

# Real Estate
df24 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing.txt", sep="\t"
)
df24["Economic_Measure_Code"] = "BEA_CA6N_1100"
df24["Published_UOM"] = "N$"
df24["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df24["Unit_of_Measure_Code"] = "R$"
df24["Default_Scale"] = "-3"
df24["Calculation_Type"] = "Level"

# Professional Scientific
df25 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Professional_Scientific_and_Technical_Services.txt",
    sep="\t",
)
df25["Economic_Measure_Code"] = "BEA_CA6N_1200"
df25["Published_UOM"] = "N$"
df25["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df25["Unit_of_Measure_Code"] = "R$"
df25["Default_Scale"] = "-3"
df25["Calculation_Type"] = "Level"

# Management of Companies
df26 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Management_of_Companies_and_Enterprises.txt", sep="\t"
)
df26["Economic_Measure_Code"] = "BEA_CA6N_1300"
df26["Published_UOM"] = "N$"
df26["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df26["Unit_of_Measure_Code"] = "R$"
df26["Default_Scale"] = "-3"
df26["Calculation_Type"] = "Level"

# Admin Support
df27 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt",
    sep="\t",
)
df27["Economic_Measure_Code"] = "BEA_CA6N_1400"
df27["Published_UOM"] = "N$"
df27["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df27["Unit_of_Measure_Code"] = "R$"
df27["Default_Scale"] = "-3"
df27["Calculation_Type"] = "Level"

# Educational Services
df28 = pd.read_csv("./Updates/STG_BEA_CA6N_Educational_Services.txt", sep="\t")
df28["Economic_Measure_Code"] = "BEA_CA6N_1500"
df28["Published_UOM"] = "N$"
df28["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df28["Unit_of_Measure_Code"] = "R$"
df28["Default_Scale"] = "-3"
df28["Calculation_Type"] = "Level"

# Health Care
df29 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Health_Care_and_Social_Assistance.txt", sep="\t"
)
df29["Economic_Measure_Code"] = "BEA_CA6N_1600"
df29["Published_UOM"] = "N$"
df29["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df29["Unit_of_Measure_Code"] = "R$"
df29["Default_Scale"] = "-3"
df29["Calculation_Type"] = "Level"

# Arts Entertainment
df30 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Arts_Entertainment_and_Recreation.txt", sep="\t"
)
df30["Economic_Measure_Code"] = "BEA_CA6N_1700"
df30["Published_UOM"] = "N$"
df30["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df30["Unit_of_Measure_Code"] = "R$"
df30["Default_Scale"] = "-3"
df30["Calculation_Type"] = "Level"

# Accommodation
df31 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Accommodation_and_Food_Services.txt", sep="\t"
)
df31["Economic_Measure_Code"] = "BEA_CA6N_1800"
df31["Published_UOM"] = "N$"
df31["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df31["Unit_of_Measure_Code"] = "R$"
df31["Default_Scale"] = "-3"
df31["Calculation_Type"] = "Level"

# Other Services
df32 = pd.read_csv("./Updates/STG_BEA_CA6N_Other_Services.txt", sep="\t")
df32["Economic_Measure_Code"] = "BEA_CA6N_1900"
df32["Published_UOM"] = "N$"
df32["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df32["Unit_of_Measure_Code"] = "R$"
df32["Default_Scale"] = "-3"
df32["Calculation_Type"] = "Level"

# Government
df33 = pd.read_csv(
    "./Updates/STG_BEA_CA6N_Government_and_Government_Enterprises.txt", sep="\t"
)
df33["Economic_Measure_Code"] = "BEA_CA6N_2000"
df33["Published_UOM"] = "N$"
df33["Estimation_Qualifier"] = "Real 2012 Chained Dollars"
df33["Unit_of_Measure_Code"] = "R$"
df33["Default_Scale"] = "-3"
df33["Calculation_Type"] = "Level"

df_list = [
    df2,
    df3,
    df4,
    df5,
    df6,
    df7,
    df8,
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
]
df_bea = df1.append(df_list)

df_bea = df_bea.drop(
    ["Region", "TableName", "LineCode", "IndustryClassification", "Unit"], axis=1
)

df_bea = df_bea.rename(
    columns={
        "Description": "Economic_Measure_Name",
        "GeoName": "GeoArea_Name",
        "GeoFIPS": "GeoArea_FIPS",
    }
)

df_bea = df_bea.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Published_UOM",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
        "Estimation_Qualifier",
    ],
    var_name="Date",
    value_name="Estimated_Real_Value",
)

df_bea["GeoArea_FIPS"] = df_bea["GeoArea_FIPS"].str.replace('"', "")

df_bea["PZ_Name"] = ""
df_bea["WDB_Name"] = ""
df_bea["Data_Period_Type"] = "YR"

df_bea["Date"] = pd.to_datetime(df_bea["Date"], errors="coerce")
df_bea["Data_Period_Business_Key"] = df_bea["Date"].dt.strftime("%Y")
df_bea["Data_Period_Name"] = df_bea["Date"].dt.strftime("The Year %Y")


#### FRED ####

# Unemployment Rate by County
df34 = pd.read_csv(
    "./Updates/STG_FRED_Unemployment_Rate_by_County_Percent.txt", sep="\t"
)
df34["Economic_Measure_Code"] = "FRED_LAUCN_003A"
df34["Economic_Measure_Name"] = "Unemployment Rate By County (Percent)"
df34["Published_UOM"] = "PCT"
df34["Unit_of_Measure_Code"] = "PCT"
df34["Calculation_Type"] = "Percent"
df34["Default_Scale"] = "2"
df34["Estimation_Qualifier"] = ""

df34 = df34.rename(
    columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"}
)

df34 = df34.drop(columns="Series ID", axis=1)

df34 = df34.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Published_UOM",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
        "Estimation_Qualifier",
    ],
    var_name="Date",
    value_name="Estimated_Real_Value",
)

# Civilian Labor Force
df35 = pd.read_csv(
    "./Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt", sep="\t"
)
df35["Economic_Measure_Code"] = "FRED_LFN_00000"
df35["Economic_Measure_Name"] = "Civilian Labor Force by County"
df35["Published_UOM"] = "PRS"
df35["Unit_of_Measure_Code"] = "PRS"
df35["Calculation_Type"] = "Level"
df35["Default_Scale"] = "0"
df35["Estimation_Qualifier"] = ""

df35 = df35.rename(
    columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"}
)

df35 = df35.drop(columns="Series ID", axis=1)

df35 = df35.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Published_UOM",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
        "Estimation_Qualifier",
    ],
    var_name="Date",
    value_name="Estimated_Real_Value",
)

# Resident Population
df36 = pd.read_csv(
    "./Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt",
    sep="\t",
)
df36["Economic_Measure_Code"] = "FRED_POP_00000"
df36["Economic_Measure_Name"] = "Resident Population by County"
df36["Published_UOM"] = "PRS"
df36["Unit_of_Measure_Code"] = "PRS"
df36["Calculation_Type"] = "Level"
df36["Default_Scale"] = "-3"
df36["Estimation_Qualifier"] = ""

df36 = df36.rename(
    columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"}
)

df36 = df36.drop(columns="Series ID", axis=1)

df36 = df36.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Published_UOM",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
        "Estimation_Qualifier",
    ],
    var_name="Date",
    value_name="Estimated_Real_Value",
)

# People 25 Years Education
df37 = pd.read_csv(
    "./Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt",
    sep="\t",
)
df37["Economic_Measure_Code"] = "FRED_S1501ACSTOTAL0_00000"
df37[
    "Economic_Measure_Name"
] = "People 25 Years and Over Who Have Completed an Associate's Degree or Higher (5-year estimate) by County (Percent)"
df37["Published_UOM"] = "PCT"
df37["Unit_of_Measure_Code"] = "PCT"
df37["Calculation_Type"] = "Percent"
df37["Default_Scale"] = "2"
df37["Estimation_Qualifier"] = ""

df37 = df37.rename(
    columns={"Region Name": "GeoArea_Name", "Region Code": "GeoArea_FIPS"}
)

df37 = df37.drop(columns="Series ID", axis=1)

df37 = df37.melt(
    id_vars=[
        "GeoArea_FIPS",
        "GeoArea_Name",
        "Economic_Measure_Code",
        "Economic_Measure_Name",
        "Published_UOM",
        "Unit_of_Measure_Code",
        "Calculation_Type",
        "Default_Scale",
        "Estimation_Qualifier",
    ],
    var_name="Date",
    value_name="Estimated_Real_Value",
)

df_fred_list = [df35, df36, df37]
df_fred = df34.append(df_fred_list)

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
df = df_bea.append(df_fred)

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

# Cumberlabor County
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

df["Estimated_Real_Value"] = df["Estimated_Real_Value"].replace("(D)", np.NaN)


# Get GDP Deflator Values
deflator = pd.read_csv(
    "https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=GDPDEF&scale=left&cosd=1947-01-01&coed=2020-04-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-10-15&revision_date=2020-10-15&nd=1947-01-01"
)
deflator = deflator.loc[:72]
deflator["Data_Period_Business_Key"] = deflator["DATE"].str[:4]
deflator = deflator.drop(columns="DATE", axis=1)


join = df.merge(deflator, on="Data_Period_Business_Key", how="left")
join["GDPDEF"] = join["GDPDEF"].astype(float)
join["Estimated_Real_Value"] = join["Estimated_Real_Value"].astype(float)
join["Real_$_Value"] = (join["Estimated_Real_Value"] / join["GDPDEF"]) * 100
join["Real_$_Value"] = join["Real_$_Value"].astype(str)

join = join.drop("GDPDEF", axis=1)

df = join

columns = [
    "GeoArea_FIPS",
    "GeoArea_Name",
    "Economic_Measure_Code",
    "Economic_Measure_Name",
    "Data_Period_Business_Key",
    "Real_$_Value",
    "Published_UOM",
    "Estimated_Real_Value",
    "Estimation_Qualifier",
    "Default_Scale",
    "Data_Period_Type",
    "Data_Period_Name",
    "Data_Period_Begin_Datetime",
    "Calculation_Type",
    "PZ_Name",
    "WDB_Name",
]

df = df[columns]

df.set_index("GeoArea_FIPS", inplace=True)

df.to_csv("./Updates/STG_WNCD_Labor_Data_Series.txt", sep="\t")
