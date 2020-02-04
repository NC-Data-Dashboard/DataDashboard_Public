#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Imports
import pandas as pd
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile
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


# Create Backups
df_backup = pd.read_csv('./Updates/STG_BEA_CAINC5N_NC.txt', encoding = 'ISO-8859-1', sep='\t')
df_backup.to_csv('./Backups/STG_BEA_CAINC5N_NC_BACKUP.txt')


# In[ ]:


# Load BEA CAINC5N_NC data
response = requests.get('https://apps.bea.gov/regional/zip/CAINC5N.zip')
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")


# In[ ]:


# Check for unused fields
df.tail(10)


# In[ ]:


# Remove unused fields
df.drop(df.tail(4).index,inplace=True)
df.tail()


# In[ ]:


#Clean GeoFIPS
df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})
df


# In[ ]:


# Set GeoFIPS as Index
df.set_index(df['GeoFIPS'], inplace = True)
df.head()


# In[ ]:


# Drop GeoFIPS column
df.drop('GeoFIPS', axis = 1, inplace = True)
df.head()


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df.to_csv('./Updates/STG_BEA_CAINC5N_NC.txt', sep = '\t')


# In[ ]:


#Reset Index for upload to database
df = df.reset_index()    


# In[ ]:


#Fill NaN values for upload to database
column_list = df.columns.values
for i in column_list:
    df.loc[df[i].isnull(),i]=0


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
c.execute('drop table STG_BEA_CAINC5N_NC_BACKUP')


# In[ ]:


#Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_CAINC5N_NC','STG_BEA_CAINC5N_NC_BACKUP';''')


# In[ ]:


c.execute('''USE [DataDashboard]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CAINC5N_NC](
	[GeoFIPS] [varchar](12) NULL,
	[GeoName] [varchar](14) NULL,
	[Region] [real] NULL,
	[TableName] [varchar](7) NULL,
	[LineCode] [real] NULL,
	[IndustryClassification] [varchar](3) NULL,
	[Description] [varchar](38) NULL,
	[Unit] [varchar](20) NULL,
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
    [2025] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=TITANIUM-BOOK;'
                                 r'Database=DataDashboard;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#df: pandas.dataframe; mTableName:table name in MS SQL
#warning: discard old table if exists
df.to_sql('STG_BEA_CAINC5N_NC', con=engine, if_exists='replace', index=False)

