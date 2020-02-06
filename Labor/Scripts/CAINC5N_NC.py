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


# In[ ]:


#Clean GeoFIPS
df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})


# In[ ]:


# Set GeoFIPS as Index
df.set_index(df['GeoFIPS'], inplace = True)


# In[ ]:


# Drop GeoFIPS column
df.drop('GeoFIPS', axis = 1, inplace = True)


# In[ ]:


#Connect to database and create cursor
con = pyodbc.connect('Driver={SQL Server};'
                      'Server=TITANIUM-BOOK;'
                      'Database=DataDashboard;'
                      'Trusted_Connection=yes;',
                    autocommit=True)

c = con.cursor()


# # Create Per Capita Personal Income

# In[ ]:


print('Updating Per Capita Personal Income...')


# In[ ]:


# Create Backups
df_pc_backup = pd.read_csv('./Updates/STG_BEA_Per_Capita_Personal_Income.txt', encoding = 'ISO-8859-1', sep='\t')
df_pc_backup.to_csv('./Backups/STG_BEA_Per_Capita_Personal_Income_BACKUP.txt')


# In[ ]:


# Create new dataframe for Per capita personal income
filter1 = df['LineCode'] == 30
df_per_capita = df[filter1]
df_per_capita.head()


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_per_capita.to_csv('./Updates/STG_BEA_Per_Capita_Personal_Income.txt', sep = '\t')


# In[ ]:


# Reset the index
df_per_capita = df_per_capita.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_per_capita.columns.values
for i in column_list:
    df_per_capita.loc[df_per_capita[i].isnull(),i]=0


# In[ ]:


df_per_capita.head()


# In[ ]:


# Drop old backup table
c.execute('drop table STG_BEA_Per_Capita_Personal_Income_BACKUP')


# In[ ]:


# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_Per_Capita_Personal_Income','STG_BEA_Per_Capita_Personal_Income_BACKUP';''')


# In[ ]:


# Create Per Capita table
c.execute('''USE [DataDashboard]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_Per_Capita_Personal_Income](
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
df_per_capita.to_sql('STG_BEA_Per_Capita_Personal_Income', con=engine, if_exists='replace', index=False)


# # Create Earnings by Place of Work

# In[ ]:


print('Done. Updating Earnings by Place of Work...')


# In[ ]:


# Create Backups
df_e_backup = pd.read_csv('./Updates/STG_BEA_Earnings_by_Place_of_Work.txt', encoding = 'ISO-8859-1', sep='\t')
df_e_backup.to_csv('./Backups/STG_BEA_Earnings_by_Place_of_Work_BACKUP.txt')


# In[ ]:


# Create a new dataframe for Earnings by place of work
filter1 = df['LineCode'] == 35
df_earnings = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_earnings.to_csv('./Updates/STG_BEA_Earnings_by_Place_of_Work.txt', sep = '\t')


# In[ ]:


# Reset the index
df_earnings = df_earnings.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_earnings.columns.values
for i in column_list:
    df_earnings.loc[df_earnings[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
c.execute('drop table STG_BEA_Earnings_by_Place_of_Work_BACKUP')


# In[ ]:


# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_Earnings_by_Place_of_Work','STG_BEA_Earnings_by_Place_of_Work_BACKUP';''')


# In[ ]:


# Create Earnings table
c.execute('''USE [DataDashboard]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_Earnings_by_Place_of_Work](
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
df_earnings.to_sql('STG_BEA_Earnings_by_Place_of_Work', con=engine, if_exists='replace', index=False)


# # Create Population

# In[ ]:


print('Done. Updating Population...')


# In[ ]:


# Create Backups
df_pop_backup = pd.read_csv('./Updates/STG_BEA_Population.txt', encoding = 'ISO-8859-1', sep='\t')
df_pop_backup.to_csv('./Backups/STG_BEA_Population_BACKUP.txt')


# In[ ]:


# Create a new dataframe for Population
filter1 = df['LineCode'] == 20
df_population = df[filter1]


# In[ ]:


# Clean Description column
df_population.loc[:,'Description'] = df_population['Description'].str.strip('2/')


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_population.to_csv('./Updates/STG_BEA_Population.txt', sep = '\t')


# In[ ]:


# Reset the index
df_population = df_population.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_population.columns.values
for i in column_list:
    df_population.loc[df_population[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
c.execute('drop table STG_BEA_Population_BACKUP')


# In[ ]:


# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_Population','STG_BEA_Population_BACKUP';''')


# In[ ]:


# Create Population table
c.execute('''USE [DataDashboard]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_Population](
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
df_population.to_sql('STG_BEA_Population', con=engine, if_exists='replace', index=False)


# # Create Personal Income

# In[ ]:


print('Done. Updating Personal Income...')


# In[ ]:


# Create Backups
df_i_backup = pd.read_csv('./Updates/STG_BEA_Personal_Income.txt', encoding = 'ISO-8859-1', sep='\t')
df_i_backup.to_csv('./Backups/STG_BEA_Personal_Income_BACKUP.txt')


# In[ ]:


# Create new dataframe for Personal Income
filter1 = df['LineCode'] == 10
df_income = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_income.to_csv('./Updates/STG_BEA_Personal_Income.txt', sep = '\t')


# In[ ]:


# Reset the index
df_income = df_income.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_income.columns.values
for i in column_list:
    df_income.loc[df_income[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
c.execute('drop table STG_BEA_Personal_Income_BACKUP')


# In[ ]:


# Create new backup
c.execute('''sp_rename 'dbo.STG_BEA_Personal_Income','STG_BEA_Personal_Income_BACKUP';''')


# In[ ]:


# Create Personal Income Table
c.execute('''USE [DataDashboard]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_Personal_Income](
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
df_income.to_sql('STG_BEA_Personal_Income', con=engine, if_exists='replace', index=False)

