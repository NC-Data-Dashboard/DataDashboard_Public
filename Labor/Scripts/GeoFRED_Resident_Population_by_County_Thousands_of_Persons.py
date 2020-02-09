#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Imports
import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
import numpy as np


# In[ ]:


# Watermark
#print('Nathan Young\nJunior Data Analyst\nCenter for the Study of Free Enterprise')
#get_ipython().run_line_magic('load_ext', 'watermark')
#get_ipython().run_line_magic('watermark', '-a "Western Carolina University" -u -d -p pandas')


# In[ ]:


# Create backups
df_backup = pd.read_csv('./Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt')
df_backup.to_csv('./Backups/STG_FRED_Resident_Population_by_County_Thoudands_of_Persons_BACKUP.txt')


# In[ ]:


# Getting and reading new data 
df = pd.read_excel("https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.78&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1549&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Thousands+of+Persons&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1970-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=582&hideLegend=false", skiprows=1)
df.head(2)


# In[ ]:


# Filter data to display only North Carolina
filter1 = df['Region Name'].str.contains(', NC')
df_nc = df[filter1]
df_nc.head(2)


# In[ ]:


# Set index to Series ID
df_nc.set_index(df_nc['Series ID'], inplace = True)
df_nc.head(2)


# In[ ]:


# Drop Series ID column
df_nc.drop('Series ID', axis = 1, inplace = True)
df_nc.head(2)


# In[ ]:


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv('./Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt', sep = '\t')


# In[ ]:


#Reset Index for upload to database
df_nc = df_nc.reset_index()    


# In[ ]:


column_list = df_nc.columns.values
for i in column_list:
    df_nc.loc[df_nc[i].isnull(),i]=0


# In[ ]:


#Connect to database and create cursor
con = pyodbc.connect('Driver={SQL Server};'
                      'Server=TITANIUM-BOOK;'
                      'Database=DataDashboard;'
                      'Trusted_Connection=yes;',
                    autocommit=True)

c = con.cursor()


# In[ ]:


#Drop old backup table
c.execute('drop table STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP')


# In[ ]:


#Create new backup
c.execute('''sp_rename 'dbo.STG_FRED_Resident_Population_by_County_Thousands_of_Persons','STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP';''')


# In[ ]:


c.execute('''USE [DataDashboard]

SET ANSI_NULLS ON


SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_FRED_Resident_Population_by_County_Thousands_of_Persons](
	[Series ID] [varchar](14) NULL,
	[Region Name] [varchar](23) NULL,
	[Region Code] [int] NULL,
	[1975] [float] NULL,
	[1976] [float] NULL,
	[1977] [float] NULL,
	[1978] [float] NULL,
	[1979] [float] NULL,
	[1980] [float] NULL,
	[1981] [float] NULL,
	[1982] [float] NULL,
	[1983] [float] NULL,
	[1984] [float] NULL,
	[1985] [float] NULL,
	[1986] [float] NULL,
	[1987] [float] NULL,
	[1988] [float] NULL,
	[1989] [float] NULL,
	[1990] [float] NULL,
	[1991] [float] NULL,
	[1992] [float] NULL,
	[1993] [float] NULL,
	[1994] [float] NULL,
	[1995] [float] NULL,
	[1996] [float] NULL,
	[1997] [float] NULL,
	[1998] [float] NULL,
	[1999] [float] NULL,
	[2000] [float] NULL,
	[2001] [float] NULL,
	[2002] [float] NULL,
	[2003] [float] NULL,
	[2004] [float] NULL,
	[2005] [float] NULL,
	[2006] [float] NULL,
	[2007] [float] NULL,
	[2008] [float] NULL,
	[2009] [float] NULL,
	[2010] [float] NULL,
	[2011] [float] NULL,
	[2012] [float] NULL,
	[2013] [float] NULL,
	[2014] [float] NULL,
	[2015] [float] NULL,
	[2016] [float] NULL,
	[2017] [float] NULL,
	[2018] [float] NULL,
    [2019] [float] NULL,
    [2020] [float] NULL,
    [2021] [float] NULL,
    [2022] [float] NULL,
    [2023] [float] NULL,
    [2024] [float] NULL,
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=TITANIUM-BOOK;'
                                 r'Database=DataDashboard;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_nc.to_sql('STG_FRED_Resident_Population_by_County_Thousands_of_Persons', con=engine, if_exists='replace', index=False)

