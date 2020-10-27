#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Imports
import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc


# In[ ]:

# Create backups
df_backup = pd.read_csv(
    "./Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt"
)
df_backup.to_csv(
    "./Backups/STG_FRED_Resident_Population_by_County_Thoudands_of_Persons_BACKUP.txt"
)

# In[ ]:


# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90.00&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1549&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Thousands+of+Persons%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2019-01-01&type=xls&startDate=1970-01-01&endDate=2021-01-01&mapWidth=2000&mapHeight=1214&hideLegend=false",
    skiprows=1,
)
df.head(2)


# In[ ]:


# Filter data to display only North Carolina
filter1 = df["Region Name"].str.contains(", NC")
df_nc = df[filter1]
df_nc.head(2)


# In[ ]:


# Set index to Series ID
df_nc.set_index(df_nc["Region Code"], inplace=True)
df_nc.head(2)


# In[ ]:


# Drop Series ID column
df_nc.drop("Region Code", axis=1, inplace=True)
df_nc.drop("Series ID", axis=1, inplace=True)

# In[ ]:

# Get national population data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=0&lat=40&zoom=2&showLabels=true&showValues=true&regionType=country&seriesTypeId=534&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Millions+of+Persons%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2017-01-01&type=xls&startDate=1970-01-01&endDate=2017-01-01&mapWidth=999&mapHeight=1253&hideLegend=false",
    skiprows=1,
)

filter2 = df["Region Name"] == "United States"
df_nation = df[filter2]

df_nation["Region Code"] = "00000"

df_nation.set_index(df_nation["Region Code"], inplace=True)

df_nation.drop("Region Code", axis=1, inplace=True)
df_nation.drop("Series ID", axis=1, inplace=True)

# In[ ]:

df_nc = df_nc.append(df_nation)


# In[ ]:

# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv(
    "./Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt",
    sep="\t",
)


# In[ ]:

'''
# Reset Index for upload to database
df_nc = df_nc.reset_index()


# In[ ]:


column_list = df_nc.columns.values
for i in column_list:
    df_nc.loc[df_nc[i].isnull(), i] = 0


# In[ ]:


# Connect to database and create cursor
con = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=[server];"
    "Database=[database];"
    "Trusted_Connection=yes;",
    autocommit=True,
)

c = con.cursor()


# In[ ]:


# Drop old backup table
c.execute(
    "drop table STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP"
)


# In[ ]:


# Create new backup
c.execute(
    """sp_rename 'dbo.STG_FRED_Resident_Population_by_County_Thousands_of_Persons','STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP';"""
)


# In[ ]:


params = urllib.parse.quote_plus(
    r"Driver={SQL Server};"
    r"Server=[server];"
    r"Database=[database];"
    r"Trusted_Connection=yes;"
)

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# warning: discard old table if exists
df_nc.to_sql(
    "STG_FRED_Resident_Population_by_County_Thousands_of_Persons",
    con=engine,
    if_exists="replace",
    index=False,
)
'''
