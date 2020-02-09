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


# Create Backups
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_MedianListingPricePerSqft_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_MedianListingPricePerSqft_AllHomes_BACKUP.txt')


# In[ ]:


#Load Land data
df_mlsf = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianListingPricePerSqft_AllHomes.csv', 
                      encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_mlsf.head()


# In[ ]:


#Filter data to NC
filter1 = df_mlsf['State'] == "NC"
df_mlsf_nc = df_mlsf[filter1]

#Check to ensure filter worked
df_mlsf_nc.head(5)


# In[ ]:


#View data types of dataframe
df_mlsf_nc.dtypes


# In[ ]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_mlsf_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlsf_nc['MunicipalCodeFIPS'].astype(str)
df_mlsf_nc.dtypes


# In[ ]:


#Add leading 0's and check to ensure they were added
df_mlsf_nc.loc[ :, 'MunicipalCodeFIPS'] = df_mlsf_nc['MunicipalCodeFIPS'].str.zfill(3)
df_mlsf_nc.head(5)


# In[ ]:


# Set Index to Region Name
df_mlsf_nc.set_index(df_mlsf_nc['RegionName'], inplace = True)
df_mlsf_nc


# In[ ]:


# Drop Region Name column
df_mlsf_nc.drop('RegionName', axis = 1, inplace = True)
df_mlsf_nc


# In[ ]:


#Save to csv file for export in Excel
df_mlsf_nc.to_csv('./Updates/STG_ZLLW_County_MedianListingPricePerSqf_AllHomes.txt', sep = '\t')


# In[ ]:


#Reset Index for upload to database
df_mlsf_nc = df_mlsf_nc.reset_index()    


# In[ ]:


#Fill NaN values for upload to database
df_mlsf_nc['Metro'] = df_mlsf_nc['Metro'].replace(np.nan,'', regex=True)

column_list = df_mlsf_nc.columns.values
for i in column_list:
    df_mlsf_nc.loc[df_mlsf_nc[i].isnull(),i]=0


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
c.execute('''drop table dbo.STG_ZLLW_County_MedianListingPricePerSqft_AllHomes_BACKUP;''')


# In[ ]:


c.execute('''sp_rename 'dbo.STG_ZLLW_County_MedianListingPricePerSqft_AllHomes','STG_ZLLW_County_MedianListingPricePerSqft_AllHomes_BACKUP';''')


# In[ ]:


c.execute('''USE [DataDashboard]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_ZLLW_County_MedianListingPricePerSqft_AllHomes](
	[RegionName] [varchar](40) NULL,
	[State] [varchar](2) NULL,
	[Metro] [varchar](40) NULL,
	[StateCodeFIPS] [varchar](2) NULL,
	[MunicipalCodeFIPS] [varchar](3) NULL,
	[SizeRank] [smallint] NULL,
	[2010-01] [float] NULL,
	[2010-02] [float] NULL,
	[2010-03] [float] NULL,
	[2010-04] [float] NULL,
	[2010-05] [float] NULL,
	[2010-06] [float] NULL,
	[2010-07] [float] NULL,
	[2010-08] [float] NULL,
	[2010-09] [float] NULL,
	[2010-10] [float] NULL,
	[2010-11] [float] NULL,
	[2010-12] [float] NULL,
	[2011-01] [float] NULL,
	[2011-02] [float] NULL,
	[2011-03] [float] NULL,
	[2011-04] [float] NULL,
	[2011-05] [float] NULL,
	[2011-06] [float] NULL,
	[2011-07] [float] NULL,
	[2011-08] [float] NULL,
	[2011-09] [float] NULL,
	[2011-10] [float] NULL,
	[2011-11] [float] NULL,
	[2011-12] [float] NULL,
	[2012-01] [float] NULL,
	[2012-02] [float] NULL,
	[2012-03] [float] NULL,
	[2012-04] [float] NULL,
	[2012-05] [float] NULL,
	[2012-06] [float] NULL,
	[2012-07] [float] NULL,
	[2012-08] [float] NULL,
	[2012-09] [float] NULL,
	[2012-10] [float] NULL,
	[2012-11] [float] NULL,
	[2012-12] [float] NULL,
	[2013-01] [float] NULL,
	[2013-02] [float] NULL,
	[2013-03] [float] NULL,
	[2013-04] [float] NULL,
	[2013-05] [float] NULL,
	[2013-06] [float] NULL,
	[2013-07] [float] NULL,
	[2013-08] [float] NULL,
	[2013-09] [float] NULL,
	[2013-10] [float] NULL,
	[2013-11] [float] NULL,
	[2013-12] [float] NULL,
	[2014-01] [float] NULL,
	[2014-02] [float] NULL,
	[2014-03] [float] NULL,
	[2014-04] [float] NULL,
	[2014-05] [float] NULL,
	[2014-06] [float] NULL,
	[2014-07] [float] NULL,
	[2014-08] [float] NULL,
	[2014-09] [float] NULL,
	[2014-10] [float] NULL,
	[2014-11] [float] NULL,
	[2014-12] [float] NULL,
	[2015-01] [float] NULL,
	[2015-02] [float] NULL,
	[2015-03] [float] NULL,
	[2015-04] [float] NULL,
	[2015-05] [float] NULL,
	[2015-06] [float] NULL,
	[2015-07] [float] NULL,
	[2015-08] [float] NULL,
	[2015-09] [float] NULL,
	[2015-10] [float] NULL,
	[2015-11] [float] NULL,
	[2015-12] [float] NULL,
	[2016-01] [float] NULL,
	[2016-02] [float] NULL,
	[2016-03] [float] NULL,
	[2016-04] [float] NULL,
	[2016-05] [float] NULL,
	[2016-06] [float] NULL,
	[2016-07] [float] NULL,
	[2016-08] [float] NULL,
	[2016-09] [float] NULL,
	[2016-10] [float] NULL,
	[2016-11] [float] NULL,
	[2016-12] [float] NULL,
	[2017-01] [float] NULL,
	[2017-02] [float] NULL,
	[2017-03] [float] NULL,
	[2017-04] [float] NULL,
	[2017-05] [float] NULL,
	[2017-06] [float] NULL,
	[2017-07] [float] NULL,
	[2017-08] [float] NULL,
	[2017-09] [float] NULL,
	[2017-10] [float] NULL,
	[2017-11] [float] NULL,
	[2017-12] [float] NULL,
	[2018-01] [float] NULL,
	[2018-02] [float] NULL,
	[2018-03] [float] NULL,
	[2018-04] [float] NULL,
	[2018-05] [float] NULL,
	[2018-06] [float] NULL,
	[2018-07] [float] NULL,
	[2018-08] [float] NULL,
	[2018-09] [float] NULL,
	[2018-10] [float] NULL,
	[2018-11] [float] NULL,
	[2018-12] [float] NULL,
	[2019-01] [float] NULL,
	[2019-02] [float] NULL,
	[2019-03] [float] NULL,
	[2019-04] [float] NULL,
	[2019-05] [float] NULL,
	[2019-06] [float] NULL,
	[2019-07] [float] NULL,
	[2019-08] [float] NULL,
	[2019-09] [float] NULL,
	[2019-10] [float] NULL,
	[2019-11] [float] NULL,
	[2019-12] [float] NULL,
    [2020-01] [float] NULL,
    [2020-02] [float] NULL,
    [2020-03] [float] NULL,
    [2020-04] [float] NULL,
    [2020-05] [float] NULL,
    [2020-06] [float] NULL,
    [2020-07] [float] NULL,
    [2020-08] [float] NULL,
    [2020-09] [float] NULL,
    [2020-10] [float] NULL,
    [2020-11] [float] NULL,
    [2020-12] [float] NULL,
    [2021-01] [float] NULL,
    [2021-02] [float] NULL,
    [2021-03] [float] NULL,
    [2021-04] [float] NULL,
    [2021-05] [float] NULL,
    [2021-06] [float] NULL,
    [2021-07] [float] NULL,
    [2021-08] [float] NULL,
    [2021-09] [float] NULL,
    [2021-10] [float] NULL,
    [2021-11] [float] NULL,
    [2021-12] [float] NULL,
    [2022-01] [float] NULL,
    [2022-02] [float] NULL,
    [2022-03] [float] NULL,
    [2022-04] [float] NULL,
    [2022-05] [float] NULL,
    [2022-06] [float] NULL,
    [2022-07] [float] NULL,
    [2022-08] [float] NULL,
    [2022-09] [float] NULL,
    [2022-10] [float] NULL,
    [2022-11] [float] NULL,
    [2022-12] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=TITANIUM-BOOK;'
                                 r'Database=DataDashboard;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#warning: discard old table if exists
df_mlsf_nc.to_sql('STG_ZLLW_County_MedianListingPricePerSqft_AllHomes', con=engine, if_exists='replace', index=False)

