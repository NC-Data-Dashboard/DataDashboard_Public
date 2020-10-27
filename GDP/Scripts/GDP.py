#!/usr/bin/env python
# coding: utf-8

# In[51]:


# Imports
import urllib
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine
import pyodbc


# ## Backups

# In[52]:


backup_df = pd.read_csv("./Updates/STG_WNCD_GDP_Data_Series.txt", sep="\t")
backup_df.to_csv("./Backups/STG_WNCD_GDP_Data_Series_BACKUP.txt", sep="\t")


# ## Process data

# In[53]:


response = requests.get("https://apps.bea.gov/regional/zip/CAGDP2.zip")
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    CAGDP2 = pd.read_csv(csvfile, encoding="ISO-8859-1", sep=",")

# Rename columns
CAGDP2 = CAGDP2.rename(
    columns={
        "GeoFIPS": "GeoArea_FIPS",
        "Description": "Economic_Measure_Name",
        "GeoName": "GeoArea_Name",
    }
)

CAGDP2["LineCode"] = CAGDP2["LineCode"].astype("str")

# Remove quotes from GeoFIPS
CAGDP2["GeoArea_FIPS"] = CAGDP2["GeoArea_FIPS"].str.replace('"', "")

# Add GeoArea Key column
CAGDP2["GeoArea_Key"] = "FIPS_" + CAGDP2["GeoArea_FIPS"]
CAGDP2["GeoArea_Key"] = CAGDP2["GeoArea_Key"].str.replace(" ", "")

# Change ', NC' in County values to 'County'
CAGDP2["GeoArea_Name"] = CAGDP2["GeoArea_Name"].str.replace(", NC", " County")

# Drop rows at the end of table
CAGDP2.drop(CAGDP2.tail(4).index, inplace=True)

# Remove rows that are not needed
CAGDP2 = CAGDP2.drop(
    columns=["Region", "TableName", "LineCode", "IndustryClassification", "Unit"]
)

# Strip whitespace from object type columns
# CAGDP2_obj = CAGDP2.select_dtypes(["object"])
# CAGDP2[CAGDP2_obj.columns] = CAGDP2_obj.apply(lambda x: x.str.strip())

measures = [
    "  Agriculture, forestry, fishing and hunting",
    "  Mining, quarrying, and oil and gas extraction",
    "  Utilities",
    "  Construction",
    "  Manufacturing",
    "  Wholesale trade",
    "  Retail trade",
    "  Transportation and warehousing",
    "  Information",
    "  Finance and insurance",
    "   Real estate and rental and leasing",
    "  Professional and business services",
    "   Management of companies and enterprises",
    "   Administrative and support and waste management and remediation services",
    "   Educational services",
    "   Health care and social assistance",
    "   Arts, entertainment, and recreation",
    "   Accommodation and food services",
    "  Other services (except government and government enterprises)",
    "Government and government enterprises",
]

CAGDP2 = CAGDP2.query("Economic_Measure_Name in @measures")

CAGDP2 = CAGDP2.dropna()

# Melt data
CAGDP2 = CAGDP2.melt(
    id_vars=["GeoArea_FIPS", "GeoArea_Key", "GeoArea_Name", "Economic_Measure_Name"],
    var_name="Data_Period_Business_Key",
    value_name="GDP",
)

# Replace '(D)' with Nan
CAGDP2["GDP"] = CAGDP2["GDP"].replace("(D)", np.NaN)


# In[54]:


# North Central
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Chatham|Durham|Edgecombe|Franklin|Granville|Harnett|Johnston|Lee|Nash|Orange|Person|Vance|Wake|Warren|Wilson"
    ),
    "PZ_Name",
] = "North Central"

# Northeast
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Beaufort|Bertie|Camden|Chowan|Tyrrell|Hyde|Currituck|Dare|Gates|Halifax|Hertford|Hyde|Martin|Northampton|Pasquotank|Perquimans|Pitt|Washington"
    ),
    "PZ_Name",
] = "Northeast"

# Northwest
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Avery|Alleghany|Alexander|Ashe|Burke|Caldwell|Catawba|McDowell|Mitchell|Watauga|Wilkes|Yancey"
    ),
    "PZ_Name",
] = "Northwest"

# Piedmont-Triad
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Alamance|Caswell|Davidson|Davie|Forsyth|Guilford|Randolph|Rockingham|Stokes|Surry|Yadkin"
    ),
    "PZ_Name",
] = "Piedmont-Triad"

# South Central
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Bladen|Columbus|Cumberland|Hoke|Montgomery|Moore|Richmond|Robeson|Sampson|Scotland"
    ),
    "PZ_Name",
] = "South Central"

# Southeast
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Jones|Brunswick|Carteret|Craven|Duplin|Greene|Lenoir|New Hanover|Onslow|Pamlico|Pender|Wayne"
    ),
    "PZ_Name",
] = "Southeast"

# Southwest
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Anson|Cabarrus|Cleveland|Gaston|Iredell|Lincoln|Mecklenburg|Rowan|Stanly|Union"
    ),
    "PZ_Name",
] = "Southwest"

# Western
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Graham|Buncombe|Cherokee|Clay|Haywood|Henderson|Jackson|Macon|Madison|Polk|Rutherford|Swain|Transylvania"
    ),
    "PZ_Name",
] = "Western"

# Cape Fear
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Brunswick|Columbus|New Hanover|Pender"),
    "WDB_Name",
] = "Cape Fear"

# Capital Area
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Johnson|Wake"), "WDB_Name"
] = "Capital Area"

# Centralina
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Anson|Cabarrus|Iredell|Lincoln|Rowan|Stanley|Union"
    ),
    "WDB_Name",
] = "Centralina"

# Charlotte Works
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Mecklenburgh"), "WDB_Name"
] = "Charlotte Works"

# Cumberlabor County
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Cumberland"), "WDB_Name"
] = "Cumberland County"

# DavidsonWorks, Inc.
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Davidson"), "WDB_Name"
] = "DavidsonWorks, Inc."

# Durham
CAGDP2.loc[CAGDP2["GeoArea_Name"].str.contains("Durham"), "WDB_Name"] = "Durham"

# Eastern Carolina
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Carteret|Craven|Duplin|Greene|Jones|Lenoir|Onslow|Pamlico|Wayne"
    ),
    "WDB_Name",
] = "Eastern Carolina"

# Gaston County
CAGDP2.loc[CAGDP2["GeoArea_Name"].str.contains("Gaston"), "WDB_Name"] = "Gaston County"

# Greensboro/High Point/Guilford
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Guilford"), "WDB_Name"
] = "Greensboro/High Point/Guilford"

# High Country
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Alleghany|Ashe|Yancey|Avery|Mitchell|Watauga|Wilkes"
    ),
    "WDB_Name",
] = "High Country"

# Kerr-Tar
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Caswell|Franklin|Granville|Person|Vance|Warren"
    ),
    "WDB_Name",
] = "Kerr-Tar"

# Lumber River
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Bladen|Hoke|Richmond|Roberson|Scotland"),
    "WDB_Name",
] = "Lumber River"

# Mountain Area
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Buncombe|Henderson|Madison|Transylvania"),
    "WDB_Name",
] = "Mountain Area"

# Northeastern
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Camden|Chowan|Currituck|Dare|Gates|Hyde|Pasquotank|Perquimans|Washington|Tyrrell"
    ),
    "WDB_Name",
] = "Northeastern"

# Northwest Piedmont
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Davie|Forsyth|Rockingham|Stokes|Surry|Yadkin"),
    "WDB_Name",
] = "Northwest Piedmont"

# Region C
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Cleveland|McDowell|Polk|Rutherford"),
    "WDB_Name",
] = "Region C"

# Region Q
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Beaufort|Bertie|Hertford|Martin|Pitt"),
    "WDB_Name",
] = "Region Q"

# Regional Partnership
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Alamance|Montgomery|Moore|Orange|Randolph"),
    "WDB_Name",
] = "Regional Partnership"

# Southwestern
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains(
        "Cherokee|Clay|Graham|Haywood|Macon|Jackson|Swain"
    ),
    "WDB_Name",
] = "Southwestern"

# Triangle South
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Chatham|Harnett|Sampson|Lee"), "WDB_Name"
] = "Triangle South"

# Turning Point
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Edgecombe|Halifax|Nash|Northampton|Wilson"),
    "WDB_Name",
] = "Turning Point"

# Western Piedmont
CAGDP2.loc[
    CAGDP2["GeoArea_Name"].str.contains("Alexander|Burke|Caldwell|Catawba"), "WDB_Name"
] = "Western Piedmont"

# Replace NaN in PZ_name and WDB_Name with nothing
CAGDP2["PZ_Name"] = CAGDP2["PZ_Name"].replace(np.NaN, "")
CAGDP2["WDB_Name"] = CAGDP2["WDB_Name"].replace(np.NaN, "")


# In[55]:


CAGDP2.columns


# In[56]:


columns = [
    "GeoArea_FIPS",
    "GeoArea_Key",
    "GeoArea_Name",
    "PZ_Name",
    "WDB_Name",
    "Economic_Measure_Name",
    "Data_Period_Business_Key",
    "GDP",
]

CAGDP2 = CAGDP2[columns]

CAGDP2["GeoArea_FIPS"] = CAGDP2["GeoArea_FIPS"].str.lstrip()

CAGDP2.set_index("GeoArea_FIPS", inplace=True)

# ## Load into Database

# In[ ]:


CAGDP2.to_csv("./Updates/STG_WNCD_GDP_Data_Series.txt", sep="\t")