#!/usr/bin/env python
# coding: utf-8


# Imports
import urllib
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import pyodbc


# Create backups
df_backup = pd.read_csv("./Updates/STG_ZLLW_County_MedianValuePerSqft_AllHomes.txt")
df_backup.to_csv("./Backups/STG_ZLLW_County_MedianValuePerSqft_AllHomes_BACKUP.txt")


# Load Land data
df_mvsf = pd.read_csv(
    "http://files.zillowstatic.com/research/public/County/County_MedianValuePerSqft_AllHomes.csv",
    encoding="ISO-8859-1",
)

# Display table to ensure data loaded correctly
df_mvsf.head()


# Filter data to NC
filter1 = df_mvsf["State"] == "NC"
df_mvsf_nc = df_mvsf[filter1]

# Check to ensure filter worked
df_mvsf_nc.head(5)


# View data types of dataframe
df_mvsf_nc.dtypes


# Change MunicipalCodeFIPS dtype to add leading 0's
df_mvsf_nc.loc[:, "MunicipalCodeFIPS"] = df_mvsf_nc["MunicipalCodeFIPS"].astype(str)
df_mvsf_nc.dtypes


# Add leading 0's and check to ensure they were added
df_mvsf_nc.loc[:, "MunicipalCodeFIPS"] = df_mvsf_nc["MunicipalCodeFIPS"].str.zfill(3)
df_mvsf_nc.head(5)


# Set Index to Region Name
df_mvsf_nc.set_index(df_mvsf_nc["RegionName"], inplace=True)
df_mvsf_nc


# Drop Region Name column
df_mvsf_nc.drop("RegionName", axis=1, inplace=True)
df_mvsf_nc


# Save to csv file for export in Excel
df_mvsf_nc.to_csv("./Updates/STG_ZLLW_County_MedianValuePerSqft_AllHomes.txt", sep="\t")
