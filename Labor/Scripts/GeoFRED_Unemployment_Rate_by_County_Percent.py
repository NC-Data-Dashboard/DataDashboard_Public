#!/usr/bin/env python
# coding: utf-8


# Imports
import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc


# Create backups
df_backup = pd.read_csv("./Updates/STG_FRED_Unemployment_Rate_by_County_Percent.txt")
df_backup.to_csv("./Backups/STG_FRED_Unemployment_Rate_by_County_Percent_BACKUP.txt")


# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90.00&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1224&attributes=Not+Seasonally+Adjusted%2C+Monthly%2C+Percent%2C+no_period_desc&aggregationFrequency=Monthly&aggregationType=Average&transformation=lin&date=2020-06-01&type=xls&startDate=1976-01-01&endDate=2020-06-01&mapWidth=2000&mapHeight=1214&hideLegend=false",
    skiprows=1,
)
df.head(2)


# Filter data to display only North Carolina
filter1 = df["Region Name"].str.contains(", NC")
df_nc = df[filter1]
df_nc.head(2)


# Set Index to Series ID
df_nc.set_index(df_nc["Series ID"], inplace=True)
df_nc.head(2)


# Drop Series ID column
df_nc.drop("Series ID", axis=1, inplace=True)
df_nc.head(2)


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv("./Updates/STG_FRED_Unemployment_Rate_by_County_Percent.txt", sep="\t")
