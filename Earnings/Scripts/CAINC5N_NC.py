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


# Load BEA CAINC6N_NC data
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
                      'Server=STEIN\ECONDEV;'
                      'Database=STG2;'
                      'Trusted_Connection=yes;',
                    autocommit=True)

c = con.cursor()


# # Wages and Salaries

# In[ ]:


print('Done. Updating Wages and Salaries...')


# In[ ]:


# Create Backups
df_w_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Wages_and_Salaries.txt', encoding = 'ISO-8859-1', sep='\t')
df_w_backup.to_csv('./Backups/STG_BEA_CA5N_Wages_and_Salaries_BACKUP.txt')


# In[ ]:


# Create a new dataframe for Earnings by place of work
filter1 = df['LineCode'] == 5
df_wages = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_wages.to_csv('./Updates/STG_BEA_CA5N_Wages_and_Salaries.txt', sep = '\t')


# In[ ]:


# Reset the index
df_wages = df_wages.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_wages.columns.values
for i in column_list:
    df_wages.loc[df_wages[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Wages_and_Salaries_BACKUP')


# In[ ]:


# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Wages_and_Salaries','STG_BEA_CA5N_Wages_and_Salaries_BACKUP';''')


# In[ ]:


# Create Earnings table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Wages_and_Salaries](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_wages.to_sql('STG_BEA_CA5N_Wages_and_Salaries', con=engine, if_exists='replace', index=False)


# # Health Care and Social Assistance

# In[ ]:


print('Done. Updating Health Care and Social Assistance...')


# In[ ]:


# Create Backups
df_h_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt', encoding = 'ISO-8859-1', sep='\t')
df_h_backup.to_csv('./Backups/STG_BEA_CA5N_Health_Care_and_Social_Assistance_BACKUP.txt')


# In[ ]:


# Create a new dataframe for Health_Care_and_Social_Assistance
filter1 = df['LineCode'] == 1600
df_health = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_health.to_csv('./Updates/STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt', sep = '\t')


# In[ ]:


# Reset the index
df_health = df_health.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_health.columns.values
for i in column_list:
    df_health.loc[df_health[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Health_Care_and_Social_Assistance_BACKUP')


# In[ ]:


# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Health_Care_and_Social_Assistance','STG_BEA_CA5N_Health_Care_and_Social_Assistance_BACKUP';''')


# In[ ]:


# Create Health_Care_and_Social_Assistance table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Health_Care_and_Social_Assistance](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_health.to_sql('STG_BEA_CA5N_Health_Care_and_Social_Assistance', con=engine, if_exists='replace', index=False)


# # Information

# In[ ]:


print('Done. Updating Information..')


# In[ ]:


# Create Backups
df_i_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Information.txt', encoding = 'ISO-8859-1', sep='\t')
df_i_backup.to_csv('./Backups/STG_BEA_CA5N_Information_BACKUP.txt')


# In[ ]:


# Create new dataframe for Information
filter1 = df['LineCode'] == 900
df_info = df[filter1]


# In[ ]:


# Save as tab-delimited txt file for export to SSMS
df_info.to_csv('./Updates/STG_BEA_CA5N_Information.txt', sep = '\t')


# In[ ]:


# Reset the index
df_info = df_info.reset_index()


# In[ ]:


# Fill NaN values for upload to database
column_list = df_info.columns.values
for i in column_list:
    df_info.loc[df_info[i].isnull(),i]=0


# In[ ]:


# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Information_BACKUP')


# In[ ]:


# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Information','STG_BEA_CA5N_Information_BACKUP';''')


# In[ ]:


# Create Information Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Information](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_info.to_sql('STG_BEA_CA5N_Information', con=engine, if_exists='replace', index=False)


# # Management of Companies and Enterprises

# In[ ]:


print('Done. Updating Management of Companies and Enterprises..')

# Create Backups
df_mang_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt', encoding = 'ISO-8859-1', sep='\t')
df_mang_backup.to_csv('./Backups/STG_BEA_CA5N_Management_of_Companies_and_Enterprises_BACKUP.txt')

# Create new dataframe for Information
filter1 = df['LineCode'] == 1300
df_management = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_management.to_csv('./Updates/STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt', sep = '\t')

# Reset the index
df_management = df_management.reset_index()

# Fill NaN values for upload to database
column_list = df_management.columns.values
for i in column_list:
    df_management.loc[df_management[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Management_of_Companies_and_Enterprises_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Management_of_Companies_and_Enterprises','STG_BEA_CA5N_Management_of_Companies_and_Enterprises_BACKUP';''')

# Create Information Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Management_of_Companies_and_Enterprises](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=TfITANIUM-BOOK;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_management.to_sql('STG_BEA_CA5N_Management_of_Companies_and_Enterprises', con=engine, if_exists='replace', index=False)


# # Manufacturing

# In[ ]:


print('Done. Updating Manufacturing..')

# Create Backups
df_manu_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Manufacturing.txt', encoding = 'ISO-8859-1', sep='\t')
df_manu_backup.to_csv('./Backups/STG_BEA_CA5N_Manufacturing_BACKUP.txt')

# Create new dataframe for Manufacturing
filter1 = df['LineCode'] == 500
df_manufacturing = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_manufacturing.to_csv('./Updates/STG_BEA_CA5N_Manufacturing.txt', sep = '\t')

# Reset the indexf
df_manufacturing = df_manufacturing.reset_index()

# Fill NaN values for upload to database
column_list = df_manufacturing.columns.values
for i in column_list:
    df_manufacturing.loc[df_manufacturing[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Manufacturing_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Manufacturing','STG_BEA_CA5N_Manufacturing_BACKUP';''')

# Create Manufacturing Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Manufacturing](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_manufacturing.to_sql('STG_BEA_CA5N_Manufacturing', con=engine, if_exists='replace', index=False)


# # Mining, Quarrying, and Oil and Gas Production

# In[ ]:


print('Done. Updating Mining, Quarrying, and Oil and Gas Production..')

# Create Backups
df_min_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt', encoding = 'ISO-8859-1', sep='\t')
df_min_backup.to_csv('./Backups/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP.txt')

# Create new dataframe for Mining_Quarrying_and_Oil_and_Gas_Extraction
filter1 = df['LineCode'] == 200
df_mining = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_mining.to_csv('./Updates/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt', sep = '\t')

# Reset the index
df_mining = df_mining.reset_index()

# Fill NaN values for upload to database
column_list = df_mining.columns.values
for i in column_list:
    df_mining.loc[df_mining[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction','STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction_BACKUP';''')

# Create Mining_Quarrying_and_Oil_and_Gas_Extraction Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_mining.to_sql('STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction', con=engine, if_exists='replace', index=False)


# # Other Services

# In[ ]:


print('Done. Updating Other Services..')

# Create Backups
df_ser_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Other_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_ser_backup.to_csv('./Backups/STG_BEA_CA5N_Other_Services_BACKUP.txt')

# Create new dataframe for Other_Services
filter1 = df['LineCode'] == 1900
df_services = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_services.to_csv('./Updates/STG_BEA_CA5N_Other_Services.txt', sep = '\t')

# Reset the index
df_services = df_services.reset_index()

# Fill NaN values for upload to database
column_list = df_services.columns.values
for i in column_list:
    df_services.loc[df_services[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Other_Services_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Other_Services','STG_BEA_CA5N_Other_Services_BACKUP';''')

# Create Other_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Other_Services](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_services.to_sql('STG_BEA_CA5N_Other_Services', con=engine, if_exists='replace', index=False)


# # Professional, Scientific, and Technical Services

# In[ ]:


print('Done. Updating Professional Scientific and Technical Services..')

# Create Backups
df_pst_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_pst_backup.to_csv('./Backups/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services_BACKUP.txt')

# Create new dataframe for Professional_Scientific_and_Technical_Services
filter1 = df['LineCode'] == 1200
df_professional = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_professional.to_csv('./Updates/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt', sep = '\t')

# Reset the index
df_professional = df_professional.reset_index()

# Fill NaN values for upload to database
column_list = df_professional.columns.values
for i in column_list:
    df_professional.loc[df_professional[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Professional_Scientific_and_Technical_Services_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Professional_Scientific_and_Technical_Services','STG_BEA_CA5N_Professional_Scientific_and_Technical_Services_BACKUP';''')

# Create Professional_Scientific_and_Technical_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Professional_Scientific_and_Technical_Services](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_professional.to_sql('STG_BEA_CA5N_Professional_Scientific_and_Technical_Services', con=engine, if_exists='replace', index=False)


# # Real Estate and Rental Housing

# In[ ]:


print('Done. Updating Real Estate and Rental Housing..')

# Create Backups
df_hou_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt', encoding = 'ISO-8859-1', sep='\t')
df_hou_backup.to_csv('./Backups/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing_BACKUP.txt')

# Create new dataframe for Real_Estate_and_Rental_and_Leasing
filter1 = df['LineCode'] == 1100
df_realestate = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_realestate.to_csv('./Updates/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt', sep = '\t')

# Reset the index
df_realestate = df_realestate.reset_index()

# Fill NaN values for upload to database
column_list = df_realestate.columns.values
for i in column_list:
    df_realestate.loc[df_realestate[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing','STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing_BACKUP';''')

# Create Real_Estate_and_Rental_and_Leasing Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_realestate.to_sql('STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing', con=engine, if_exists='replace', index=False)


# # Retail Trade

# In[ ]:


print('Done. Updating Retail Trade..')

# Create Backups
df_r_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Retail_Trade.txt', encoding = 'ISO-8859-1', sep='\t')
df_r_backup.to_csv('./Backups/STG_BEA_CA5N_Retail_Trade_BACKUP.txt')

# Create new dataframe for Retail_Trade
filter1 = df['LineCode'] == 700
df_retail = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_retail.to_csv('./Updates/STG_BEA_CA5N_Retail_Trade.txt', sep = '\t')

# Reset the index
df_retail = df_retail.reset_index()

# Fill NaN values for upload to database
column_list = df_retail.columns.values
for i in column_list:
    df_retail.loc[df_retail[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Retail_Trade_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Retail_Trade','STG_BEA_CA5N_Retail_Trade_BACKUP';''')

# Create Retail_Trade Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Retail_Trade](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_retail.to_sql('STG_BEA_CA5N_Retail_Trade', con=engine, if_exists='replace', index=False)


# # Transportation and Warehousing

# In[ ]:


print('Done. Updating Transportation and Warehousing..')

# Create Backups
df_t_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Transportation_and_Warehousing.txt', encoding = 'ISO-8859-1', sep='\t')
df_t_backup.to_csv('./Backups/STG_BEA_CA5N_Transportation_and_Warehousing_BACKUP.txt')

# Create new dataframe for Transportation_and_Warehousing
filter1 = df['LineCode'] == 800
df_transportation = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_transportation.to_csv('./Updates/STG_BEA_CA5N_Transportation_and_Warehousing.txt', sep = '\t')

# Reset the index
df_transportation = df_transportation.reset_index()

# Fill NaN values for upload to database
column_list = df_transportation.columns.values
for i in column_list:
    df_transportation.loc[df_transportation[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Transportation_and_Warehousing_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Transportation_and_Warehousing','STG_BEA_CA5N_Transportation_and_Warehousing_BACKUP';''')

# Create Transportation_and_Warehousing Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Transportation_and_Warehousing](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_transportation.to_sql('STG_BEA_CA5N_Transportation_and_Warehousing', con=engine, if_exists='replace', index=False)


# # Utilities

# In[ ]:


print('Done. Updating Utilities..')

# Create Backups
df_u_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Utilities.txt', encoding = 'ISO-8859-1', sep='\t')
df_u_backup.to_csv('./Backups/STG_BEA_CA5N_Utilities_BACKUP.txt')

# Create new dataframe for Utilities
filter1 = df['LineCode'] == 300
df_utilities = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_utilities.to_csv('./Updates/STG_BEA_CA5N_Utilities.txt', sep = '\t')

# Reset the index
df_utilities = df_utilities.reset_index()

# Fill NaN values for upload to database
column_list = df_utilities.columns.values
for i in column_list:
    df_utilities.loc[df_utilities[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Utilities_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Utilities','STG_BEA_CA5N_Utilities_BACKUP';''')

# Create Utilities Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Utilities](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_utilities.to_sql('STG_BEA_CA5N_Utilities', con=engine, if_exists='replace', index=False)


# # Wholesale Trade

# In[ ]:


print('Done. Updating Wholesale Trade..')

# Create Backups
df_wt_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Wholesale_Trade.txt', encoding = 'ISO-8859-1', sep='\t')
df_wt_backup.to_csv('./Backups/STG_BEA_CA5N_Wholesale_Trade_BACKUP.txt')

# Create new dataframe for Wholesale_Trade
filter1 = df['LineCode'] == 600
df_wholesale = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_wholesale.to_csv('./Updates/STG_BEA_CA5N_Wholesale_Trade.txt', sep = '\t')

# Reset the index
df_wholesale = df_wholesale.reset_index()

# Fill NaN values for upload to database
column_list = df_wholesale.columns.values
for i in column_list:
    df_wholesale.loc[df_wholesale[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Wholesale_Trade_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Wholesale_Trade','STG_BEA_CA5N_Wholesale_Trade_BACKUP';''')

# Create Wholesale_Trade Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Wholesale_Trade](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_wholesale.to_sql('STG_BEA_CA5N_Wholesale_Trade', con=engine, if_exists='replace', index=False)


# # Proprietors' Income

# In[ ]:


print('Done. Updating Proprietors Income..')

# Create Backups
df_pi_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Proprietors_Income.txt', encoding = 'ISO-8859-1', sep='\t')
df_pi_backup.to_csv('./Backups/STG_BEA_CA5N_Proprietors_Income_BACKUP.txt')

# Create new dataframe for Proprietors_Income
filter1 = df['LineCode'] == 70
df_propinc = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_propinc.to_csv('./Updates/STG_BEA_CA5N_Proprietors_Income.txt', sep = '\t')

# Reset the index
df_propinc = df_propinc.reset_index()

# Fill NaN values for upload to database
column_list = df_propinc.columns.values
for i in column_list:
    df_propinc.loc[df_propinc[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Proprietors_Income_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Proprietors_Income','STG_BEA_CA5N_Proprietors_Income_BACKUP';''')

# Create Proprietors_Income Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Proprietors_Income](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_propinc.to_sql('STG_BEA_CA5N_Proprietors_Income', con=engine, if_exists='replace', index=False)


# # Government and Government Enterprises

# In[ ]:


print('Done. Updating Government and Government Enterprises..')

# Create Backups
df_g_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Government_and_Government_Enterprises.txt', encoding = 'ISO-8859-1', sep='\t')
df_g_backup.to_csv('./Backups/STG_BEA_CA5N_Government_and_Government_Enterprises_BACKUP.txt')

# Create new dataframe for Government_and_Government_Enterprises
filter1 = df['LineCode'] == 2000
df_gov = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_gov.to_csv('./Updates/STG_BEA_CA5N_Government_and_Government_Enterprises.txt', sep = '\t')

# Reset the index
df_gov = df_gov.reset_index()

# Fill NaN values for upload to database
column_list = df_gov.columns.values
for i in column_list:
    df_gov.loc[df_gov[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Government_and_Government_Enterprises_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Government_and_Government_Enterprises','STG_BEA_CA5N_Government_and_Government_Enterprises_BACKUP';''')

# Create Government_and_Government_Enterprises Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Government_and_Government_Enterprises](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_gov.to_sql('STG_BEA_CA5N_Government_and_Government_Enterprises', con=engine, if_exists='replace', index=False)


# # Private Nonfarm Compensation

# In[ ]:


print('Done. Updating Private Nonfarm Compensation..')

# Create Backups
df_pnc_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Private_Nonfarm_Compensation.txt', encoding = 'ISO-8859-1', sep='\t')
df_pnc_backup.to_csv('./Backups/STG_BEA_CA5N_Private_Nonfarm_Compensation_BACKUP.txt')

# Create new dataframe for Private_Nonfarm_Compensation
filter1 = df['LineCode'] == 90
df_private = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_private.to_csv('./Updates/STG_BEA_CA5N_Private_Nonfarm_Compensation.txt', sep = '\t')

# Reset the index
df_private = df_private.reset_index()

# Fill NaN values for upload to database
column_list = df_private.columns.values
for i in column_list:
    df_private.loc[df_private[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Private_Nonfarm_Compensation_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Private_Nonfarm_Compensation','STG_BEA_CA5N_Private_Nonfarm_Compensation_BACKUP';''')

# Create Private_Nonfarm_Compensation Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Private_Nonfarm_Compensation](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_private.to_sql('STG_BEA_CA5N_Private_Nonfarm_Compensation', con=engine, if_exists='replace', index=False)


# # Farm Compensation

# In[ ]:


print('Done. Updating Farm Compensation..')

# Create Backups
df_fc_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Farm_Compensation.txt', encoding = 'ISO-8859-1', sep='\t')
df_fc_backup.to_csv('./Backups/STG_BEA_CA5N_Farm_Compensation_BACKUP.txt')

# Create new dataframe for Farm_Compensation
filter1 = df['LineCode'] == 81
df_farm = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_farm.to_csv('./Updates/STG_BEA_CA5N_Farm_Compensation.txt', sep = '\t')

# Reset the index
df_farm = df_farm.reset_index()

# Fill NaN values for upload to database
column_list = df_farm.columns.values
for i in column_list:
    df_farm.loc[df_farm[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Farm_Compensation_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Farm_Compensation','STG_BEA_CA5N_Farm_Compensation_BACKUP';''')

# Create Farm_Compensation Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Farm_Compensation](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_farm.to_sql('STG_BEA_CA5N_Farm_Compensation', con=engine, if_exists='replace', index=False)


# # Nonfarm Compensation

# In[ ]:


print('Done. Updating Nonfarm Compensation..')

# Create Backups
df_nf_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Nonfarm_Compensation.txt', encoding = 'ISO-8859-1', sep='\t')
df_nf_backup.to_csv('./Backups/STG_BEA_CA5N_Nonfarm_Compensation_BACKUP.txt')

# Create new dataframe for Nonfarm_Compensation
filter1 = df['LineCode'] == 82
df_nonfarm = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_nonfarm.to_csv('./Updates/STG_BEA_CA5N_Nonfarm_Compensation.txt', sep = '\t')

# Reset the index
df_nonfarm = df_nonfarm.reset_index()

# Fill NaN values for upload to database
column_list = df_nonfarm.columns.values
for i in column_list:
    df_nonfarm.loc[df_nonfarm[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Nonfarm_Compensation_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Nonfarm_Compensation','STG_BEA_CA5N_Nonfarm_Compensation_BACKUP';''')

# Create Nonfarm_Compensation Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Nonfarm_Compensation](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_nonfarm.to_sql('STG_BEA_CA5N_Nonfarm_Compensation', con=engine, if_exists='replace', index=False)


# # Supplements to Wages and Salaries

# In[ ]:


print('Done. Updating Supplements to Wages and Salaries..')

# Create Backups
df_supp_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt', encoding = 'ISO-8859-1', sep='\t')
df_supp_backup.to_csv('./Backups/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries_BACKUP.txt')

# Create new dataframe for Supplements_to_Wages_and_Salaries
filter1 = df['LineCode'] == 60
df_supplement = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_supplement.to_csv('./Updates/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt', sep = '\t')

# Reset the index
df_supplement = df_supplement.reset_index()

# Fill NaN values for upload to database
column_list = df_supplement.columns.values
for i in column_list:
    df_supplement.loc[df_supplement[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Supplements_to_Wages_and_Salaries_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Supplements_to_Wages_and_Salaries','STG_BEA_CA5N_Supplements_to_Wages_and_Salaries_BACKUP';''')

# Create Supplements_to_Wages_and_Salaries Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Supplements_to_Wages_and_Salaries](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_supplement.to_sql('STG_BEA_CA5N_Supplements_to_Wages_and_Salaries', con=engine, if_exists='replace', index=False)


# # Federal, Civilian

# In[ ]:


print('Done. Updating Govt Type Federal Civilian..')

# Create Backups
df_fcgov_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Federal_Civilian_Government.txt', encoding = 'ISO-8859-1', sep='\t')
df_fcgov_backup.to_csv('./Backups/STG_BEA_CA5N_Federal_Civilian_Government_BACKUP.txt')

# Create new dataframe for Federal_Civilian_Government
filter1 = df['LineCode'] == 2001
df_federal = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_federal.to_csv('./Updates/STG_BEA_CA5N_Federal_Civilian_Government.txt', sep = '\t')

# Reset the index
df_federal = df_federal.reset_index()

# Fill NaN values for upload to database
column_list = df_federal.columns.values
for i in column_list:
    df_federal.loc[df_federal[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Federal_Civilian_Government_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Federal_Civilian_Government','STG_BEA_CA5N_Federal_Civilian_Government_BACKUP';''')

# Create Federal_Civilian_Government Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Federal_Civilian_Government](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_federal.to_sql('STG_BEA_CA5N_Federal_Civilian_Government', con=engine, if_exists='replace', index=False)


# # Accommodation and Food Services

# In[ ]:


print('Done. Updating Accommodation and Food Services..')

# Create Backups
df_acc_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Accommodation_and_Food_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_acc_backup.to_csv('./Backups/STG_BEA_CA5N_Accommodation_and_Food_Services_BACKUP.txt')

# Create new dataframe for Accommodation_and_Food_Services
filter1 = df['LineCode'] == 1800
df_food = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_food.to_csv('./Updates/STG_BEA_CA5N_Accommodation_and_Food_Services.txt', sep = '\t')

# Reset the index
df_food = df_food.reset_index()

# Fill NaN values for upload to database
column_list = df_food.columns.values
for i in column_list:
    df_food.loc[df_food[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Accommodation_and_Food_Services_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Accommodation_and_Food_Services','STG_BEA_CA5N_Accommodation_and_Food_Services_BACKUP';''')

# Create Accommodation_and_Food_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Accommodation_and_Food_Services](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_food.to_sql('STG_BEA_CA5N_Accommodation_and_Food_Services', con=engine, if_exists='replace', index=False)


# # Administrative Support

# In[ ]:


print('Done. Updating Administrative Support..')

# Create Backups
df_as_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_as_backup.to_csv('./Backups/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP.txt')

# Create new dataframe for Administrative_and_Support_and_Waste_Management_and_Remediation_Services
filter1 = df['LineCode'] == 1400
df_admin = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_admin.to_csv('./Updates/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', sep = '\t')

# Reset the index
df_admin = df_admin.reset_index()

# Fill NaN values for upload to database
column_list = df_admin.columns.values
for i in column_list:
    df_admin.loc[df_admin[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services','STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services_BACKUP';''')

# Create Administrative_and_Support_and_Waste_Management_and_Remediation_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_admin.to_sql('STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services', con=engine, if_exists='replace', index=False)


# # Arts, Entertainment, and Recreation

# In[ ]:


print('Done. Updating Arts, Entertainment, and Recreation..')

# Create Backups
df_aer_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt', encoding = 'ISO-8859-1', sep='\t')
df_aer_backup.to_csv('./Backups/STG_BEA_CA5N_Arts_Entertainment_and_Recreation_BACKUP.txt')

# Create new dataframe for Arts_Entertainment_and_Recreation
filter1 = df['LineCode'] == 1700
df_arts = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_arts.to_csv('./Updates/STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt', sep = '\t')

# Reset the index
df_arts = df_arts.reset_index()

# Fill NaN values for upload to database
column_list = df_arts.columns.values
for i in column_list:
    df_arts.loc[df_arts[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Arts_Entertainment_and_Recreation_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Arts_Entertainment_and_Recreation','STG_BEA_CA5N_Arts_Entertainment_and_Recreation_BACKUP';''')

# Create Arts_Entertainment_and_Recreation Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Arts_Entertainment_and_Recreation](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_arts.to_sql('STG_BEA_CA5N_Arts_Entertainment_and_Recreation', con=engine, if_exists='replace', index=False)


# # Construction

# In[ ]:


print('Done. Updating Construction..')

# Create Backups
df_con_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Construction.txt', encoding = 'ISO-8859-1', sep='\t')
df_con_backup.to_csv('./Backups/STG_BEA_CA5N_Construction_BACKUP.txt')

# Create new dataframe for Construction
filter1 = df['LineCode'] == 400
df_construction = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_construction.to_csv('./Updates/STG_BEA_CA5N_Construction.txt', sep = '\t')

# Reset the index
df_construction = df_construction.reset_index()

# Fill NaN values for upload to database
column_list = df_construction.columns.values
for i in column_list:
    df_construction.loc[df_construction[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Construction_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Construction','STG_BEA_CA5N_Construction_BACKUP';''')

# Create Construction Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Construction](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_construction.to_sql('STG_BEA_CA5N_Construction', con=engine, if_exists='replace', index=False)


# # Educational Services

# In[ ]:


print('Done. Updating Educational Services..')

# Create Backups
df_es_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Educational_Services.txt', encoding = 'ISO-8859-1', sep='\t')
df_es_backup.to_csv('./Backups/STG_BEA_CA5N_Educational_Services_BACKUP.txt')

# Create new dataframe for Educational_Services
filter1 = df['LineCode'] == 1500
df_eduserv = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_eduserv.to_csv('./Updates/STG_BEA_CA5N_Educational_Services.txt', sep = '\t')

# Reset the index
df_eduserv = df_eduserv.reset_index()

# Fill NaN values for upload to database
column_list = df_eduserv.columns.values
for i in column_list:
    df_eduserv.loc[df_eduserv[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Educational_Services_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Educational_Services','STG_BEA_CA5N_Educational_Services_BACKUP';''')

# Create Educational_Services Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Educational_Services](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_eduserv.to_sql('STG_BEA_CA5N_Educational_Services', con=engine, if_exists='replace', index=False)


# # Finance and Insurance

# In[ ]:


print('Done. Updating Finance and Insurance..')

# Create Backups
df_fi_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Finance_and_Insurance.txt', encoding = 'ISO-8859-1', sep='\t')
df_fi_backup.to_csv('./Backups/STG_BEA_CA5N_Finance_and_Insurance_BACKUP.txt')

# Create new dataframe for Finance_and_Insurance
filter1 = df['LineCode'] == 1000
df_finance = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_finance.to_csv('./Updates/STG_BEA_CA5N_Finance_and_Insurance.txt', sep = '\t')

# Reset the index
df_finance = df_finance.reset_index()

# Fill NaN values for upload to database
column_list = df_finance.columns.values
for i in column_list:
    df_finance.loc[df_finance[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Finance_and_Insurance_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Finance_and_Insurance','STG_BEA_CA5N_Finance_and_Insurance_BACKUP';''')

# Create Finance_and_Insurance Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Finance_and_Insurance](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_finance.to_sql('STG_BEA_CA5N_Finance_and_Insurance', con=engine, if_exists='replace', index=False)


# # Forestry, Fishing, and Related Activities

# In[ ]:


print('Done. Updating Forestry, Fishing, and Related Activities..')

# Create Backups
df_ffr_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt', encoding = 'ISO-8859-1', sep='\t')
df_ffr_backup.to_csv('./Backups/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities_BACKUP.txt')

# Create new dataframe for Forestry_Fishing_and_Related_Activities
filter1 = df['LineCode'] == 100
df_forestry = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_forestry.to_csv('./Updates/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt', sep = '\t')

# Reset the index
df_forestry = df_forestry.reset_index()

# Fill NaN values for upload to database
column_list = df_forestry.columns.values
for i in column_list:
    df_forestry.loc[df_forestry[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities','STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities_BACKUP';''')

# Create Forestry_Fishing_and_Related_Activities Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_forestry.to_sql('STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities', con=engine, if_exists='replace', index=False)


# # Military

# In[ ]:


print('Done. Updating Govt Type Federal Civilian..')

# Create Backups
df_mgov_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Military_Government.txt', encoding = 'ISO-8859-1', sep='\t')
df_mgov_backup.to_csv('./Backups/STG_BEA_CA5N_Military_Government_BACKUP.txt')

# Create new dataframe for Military_Government
filter1 = df['LineCode'] == 2001
df_military = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_military.to_csv('./Updates/STG_BEA_CA5N_Military_Government.txt', sep = '\t')

# Reset the index
df_military = df_military.reset_index()

# Fill NaN values for upload to database
column_list = df_military.columns.values
for i in column_list:
    df_military.loc[df_military[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Military_Government_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Military_Government','STG_BEA_CA5N_Military_Government_BACKUP';''')

# Create Military_Government Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Military_Government](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_military.to_sql('STG_BEA_CA5N_Military_Government', con=engine, if_exists='replace', index=False)


# # State and Local

# In[ ]:


print('Done. Updating Govt Type State Local..')

# Create Backups
df_slgov_backup = pd.read_csv('./Updates/STG_BEA_CA5N_State_Local_Government.txt', encoding = 'ISO-8859-1', sep='\t')
df_slgov_backup.to_csv('./Backups/STG_BEA_CA5N_State_Local_Government_BACKUP.txt')

# Create new dataframe for State_Local_Government
filter1 = df['LineCode'] == 2001
df_state_local = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_state_local.to_csv('./Updates/STG_BEA_CA5N_State_Local_Government.txt', sep = '\t')

# Reset the index
df_state_local = df_state_local.reset_index()

# Fill NaN values for upload to database
column_list = df_state_local.columns.values
for i in column_list:
    df_state_local.loc[df_state_local[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_State_Local_Government_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_State_Local_Government','STG_BEA_CA5N_State_Local_Government_BACKUP';''')

# Create State_Local_Government Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_State_Local_Government](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_state_local.to_sql('STG_BEA_CA5N_State_Local_Government', con=engine, if_exists='replace', index=False)


# # State Government

# In[ ]:


print('Done. Updating Govt Type State..')

# Create Backups
df_sgov_backup = pd.read_csv('./Updates/STG_BEA_CA5N_State_Government.txt', encoding = 'ISO-8859-1', sep='\t')
df_sgov_backup.to_csv('./Backups/STG_BEA_CA5N_State_Government_BACKUP.txt')

# Create new dataframe for State_Government
filter1 = df['LineCode'] == 2001
df_state = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_state.to_csv('./Updates/STG_BEA_CA5N_State_Government.txt', sep = '\t')

# Reset the index
df_state = df_state.reset_index()

# Fill NaN values for upload to database
column_list = df_state.columns.values
for i in column_list:
    df_state.loc[df_state[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_State_Government_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_State_Government','STG_BEA_CA5N_State_Government_BACKUP';''')

# Create State_Government Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_State_Government](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_state.to_sql('STG_BEA_CA5N_State_Government', con=engine, if_exists='replace', index=False)


# # Local Government

# In[ ]:


print('Done. Updating Govt Type Local..')

# Create Backups
df_lgov_backup = pd.read_csv('./Updates/STG_BEA_CA5N_Local_Government.txt', encoding = 'ISO-8859-1', sep='\t')
df_lgov_backup.to_csv('./Backups/STG_BEA_CA5N_Local_Government_BACKUP.txt')

# Create new dataframe for Local_Government
filter1 = df['LineCode'] == 2001
df_local = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_local.to_csv('./Updates/STG_BEA_CA5N_Local_Government.txt', sep = '\t')

# Reset the index
df_local = df_local.reset_index()

# Fill NaN values for upload to database
column_list = df_local.columns.values
for i in column_list:
    df_local.loc[df_local[i].isnull(),i]=0

# Drop old backup table
#c.execute('drop table STG_BEA_CA5N_Local_Government_BACKUP')

# Create new backup
#c.execute('''sp_rename 'dbo.STG_BEA_CA5N_Local_Government','STG_BEA_CA5N_Local_Government_BACKUP';''')

# Create Local_Government Table
c.execute('''USE [STG2]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_BEA_CA5N_Local_Government](
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
    [2025] [float] NULL,
    [2026] [float] NULL,
    [2027] [float] NULL,
    [2028] [float] NULL,
    [2029] [float] NULL,
    [2030] [float] NULL
) ON [PRIMARY]''')

params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=STEIN\ECONDEV;'
                                 r'Database=STG2;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


#warning: discard old table if exists
df_local.to_sql('STG_BEA_CA5N_Local_Government', con=engine, if_exists='replace', index=False)

