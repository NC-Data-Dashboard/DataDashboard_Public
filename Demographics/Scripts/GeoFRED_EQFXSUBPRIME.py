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
df_backup = pd.read_csv('./Updates/STG_FRED_EQFXSUBPRIME.txt')
df_backup.to_csv('./Backups/STG_FRED_EQFXSUBPRIME_BACKUP.txt')


# In[ ]:


# Getting and reading new data 
df = pd.read_excel("https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147149&attributes=Not+Seasonally+Adjusted%2C+Quarterly%2C+Percent&aggregationFrequency=Quarterly&aggregationType=Average&transformation=lin&date=2025-01-01&type=xls&startDate=1999-01-01&endDate=2025-01-01&mapWidth=999&mapHeight=521&hideLegend=false", skiprows = 1)
df.head()


# In[ ]:


# Filter data to display only North Carolina
filter1 = df['Region Name'].str.contains(', NC')
df_nc = df[filter1]
df_nc.head()


# In[ ]:


# Set Series ID as index
df_nc.set_index(df_nc['Series ID'], inplace = True)
df_nc.head(2)


# In[ ]:


# Drop Series ID column
df_nc.drop('Series ID', axis = 1, inplace = True)
df_nc.head(2)


# In[ ]:


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv('./Updates/STG_FRED_EQFXSUBPRIME.txt', sep = '\t', encoding = 'UTF-8')


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
c.execute('drop table STG_FRED_EQFXSUBPRIME_BACKUP')


# In[ ]:


#Create new backup
c.execute('''sp_rename 'dbo.STG_FRED_EQFXSUBPRIME','STG_FRED_EQFXSUBPRIME_BACKUP';''')


# In[ ]:


c.execute('''USE [DataDashboard]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_FRED_EQFXSUBPRIME](
	[Series ID] [varchar](18) NULL,
	[Region Name] [varchar](23) NULL,
	[Region Code] [int] NULL,
	[1999 Q1] [float] NULL,
	[1999 Q2] [float] NULL,
	[1999 Q3] [float] NULL,
	[1999 Q4] [float] NULL,
	[2000 Q1] [float] NULL,
	[2000 Q2] [float] NULL,
	[2000 Q3] [float] NULL,
	[2000 Q4] [float] NULL,
	[2001 Q1] [float] NULL,
	[2001 Q2] [float] NULL,
	[2001 Q3] [float] NULL,
	[2001 Q4] [float] NULL,
	[2002 Q1] [float] NULL,
	[2002 Q2] [float] NULL,
	[2002 Q3] [float] NULL,
	[2002 Q4] [float] NULL,
	[2003 Q1] [float] NULL,
	[2003 Q2] [float] NULL,
	[2003 Q3] [float] NULL,
	[2003 Q4] [float] NULL,
	[2004 Q1] [float] NULL,
	[2004 Q2] [float] NULL,
	[2004 Q3] [float] NULL,
	[2004 Q4] [float] NULL,
	[2005 Q1] [float] NULL,
	[2005 Q2] [float] NULL,
	[2005 Q3] [float] NULL,
	[2005 Q4] [float] NULL,
	[2006 Q1] [float] NULL,
	[2006 Q2] [float] NULL,
	[2006 Q3] [float] NULL,
	[2006 Q4] [float] NULL,
	[2007 Q1] [float] NULL,
	[2007 Q2] [float] NULL,
	[2007 Q3] [float] NULL,
	[2007 Q4] [float] NULL,
	[2008 Q1] [float] NULL,
	[2008 Q2] [float] NULL,
	[2008 Q3] [float] NULL,
	[2008 Q4] [float] NULL,
	[2009 Q1] [float] NULL,
	[2009 Q2] [float] NULL,
	[2009 Q3] [float] NULL,
	[2009 Q4] [float] NULL,
	[2010 Q1] [float] NULL,
	[2010 Q2] [float] NULL,
	[2010 Q3] [float] NULL,
	[2010 Q4] [float] NULL,
	[2011 Q1] [float] NULL,
	[2011 Q2] [float] NULL,
	[2011 Q3] [float] NULL,
	[2011 Q4] [float] NULL,
	[2012 Q1] [float] NULL,
	[2012 Q2] [float] NULL,
	[2012 Q3] [float] NULL,
	[2012 Q4] [float] NULL,
	[2013 Q1] [float] NULL,
	[2013 Q2] [float] NULL,
	[2013 Q3] [float] NULL,
	[2013 Q4] [float] NULL,
	[2014 Q1] [float] NULL,
	[2014 Q2] [float] NULL,
	[2014 Q3] [float] NULL,
	[2014 Q4] [float] NULL,
	[2015 Q1] [float] NULL,
	[2015 Q2] [float] NULL,
	[2015 Q3] [float] NULL,
	[2015 Q4] [float] NULL,
	[2016 Q1] [float] NULL,
	[2016 Q2] [float] NULL,
	[2016 Q3] [float] NULL,
	[2016 Q4] [float] NULL,
	[2017 Q1] [float] NULL,
	[2017 Q2] [float] NULL,
	[2017 Q3] [float] NULL,
	[2017 Q4] [float] NULL,
	[2018 Q1] [float] NULL,
	[2018 Q2] [float] NULL,
	[2018 Q3] [float] NULL,
	[2018 Q4] [float] NULL,
	[2019 Q1] [float] NULL,
	[2019 Q2] [float] NULL,
    [2019 Q3] [float] NULL,
    [2019 Q4] [float] NULL,
    [2020 Q1] [float] NULL,
    [2020 Q2] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=TITANIUM-BOOK;'
                                 r'Database=DataDashboard;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#df: pandas.dataframe; mTableName:table name in MS SQL
#warning: discard old table if exists
df_nc.to_sql('STG_FRED_EQFXSUBPRIME', con=engine, if_exists='replace', index=False)

