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
df_backup = pd.read_csv("./Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt")
df_backup.to_csv("./Backups/STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP.txt")


# In[ ]:


# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90&lat=40&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=656&attributes=Not+Seasonally+Adjusted%2C+Monthly%2C+Persons%2C+no_period_desc&aggregationFrequency=Monthly&aggregationType=Average&transformation=lin&date=2020-06-01&type=xls&startDate=1990-01-01&endDate=2020-06-01&mapWidth=2000&mapHeight=1262&hideLegend=false",
    skiprows=1,
)
df.head()


# In[ ]:


# Filter data to display only North Carolina
filter1 = df["Region Name"].str.contains(", NC")
df_nc = df[filter1]
df_nc.head()


# In[ ]:


# Set Series ID as index
df_nc.set_index(df_nc["Region Code"], inplace=True)
df_nc.head(2)


# In[ ]:


# Drop Series ID column
df_nc.drop("Region Code", axis=1, inplace=True)
df_nc.drop("Series ID", axis=1, inplace=True)
df_nc.head(2)


# In[ ]:


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv("./Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt", sep="\t")


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
c.execute("drop table STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP")


# In[ ]:


# Create new backup
c.execute(
    """sp_rename 'dbo.STG_FRED_Civilian_Labor_Force_by_County_Persons','STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP';"""
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
    "STG_FRED_Civilian_Labor_Force_by_County_Persons",
    con=engine,
    if_exists="replace",
    index=False,
)
'''
