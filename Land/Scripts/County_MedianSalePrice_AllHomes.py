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


# Create Backups
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_MedianSalePrice_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_MedianSalePrice_AllHomes_BACKUP.txt')


# In[ ]:


#Load Land data
df = pd.read_csv('http://files.zillowstatic.com/research/public/County/Sale_Prices_County.csv',
                     encoding='ISO-8859-1')


# In[ ]:


df = df.drop(columns = ['RegionID'], axis = 1)


# In[ ]:


#Filter data to NC
filter1 = df['StateName'] == "North Carolina"
df_nc = df[filter1]


# In[ ]:


#Sort by Region Name
df_nc = df_nc.sort_values('RegionName', ascending = True)


# In[ ]:


df_fips = pd.read_csv('./FIPS_Codes.csv')


# In[ ]:


df_msp_nc = df_nc.set_index('RegionName').join(df_fips.set_index('RegionName'))


# In[ ]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_msp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_msp_nc['MunicipalCodeFIPS'].astype(str)


# In[ ]:


#Add leading 0's and check to ensure they were added
df_msp_nc.loc[ :, 'MunicipalCodeFIPS'] = df_msp_nc['MunicipalCodeFIPS'].str.zfill(3)


# In[ ]:


columns = ['State','Metro','StateCodeFIPS','MunicipalCodeFIPS','SizeRank','2008-03','2008-04','2008-05','2008-06','2008-07','2008-08','2008-09','2008-10','2008-11','2008-12','2009-01','2009-02','2009-03','2009-04','2009-05','2009-06','2009-07','2009-08','2009-09','2009-10','2009-11','2009-12','2010-01','2010-02','2010-03','2010-04','2010-05','2010-06','2010-07','2010-08','2010-09','2010-10','2010-11','2010-12','2011-01','2011-02','2011-03','2011-04','2011-05','2011-06','2011-07','2011-08','2011-09','2011-10','2011-11','2011-12','2012-01','2012-02','2012-03','2012-04','2012-05','2012-06','2012-07','2012-08','2012-09','2012-10','2012-11','2012-12','2013-01','2013-02','2013-03','2013-04','2013-05','2013-06','2013-07','2013-08','2013-09','2013-10','2013-11','2013-12','2014-01','2014-02','2014-03','2014-04','2014-05','2014-06','2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02','2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11', '2015-12','2016-01','2016-02','2016-03','2016-04','2016-05','2016-06','2016-07','2016-08','2016-09','2016-10','2016-11','2016-12','2017-01','2017-02','2017-03','2017-04','2017-05','2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01','2018-02','2018-03','2018-04','2018-05','2018-06','2018-07','2018-08','2018-09','2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01', '2020-02']
df_msp_nc = df_msp_nc[columns]


# In[ ]:


#Save to csv file for export in Excel
df_msp_nc.to_csv('./Updates/STG_ZLLW_County_MedianSalePrice_AllHomes.txt', sep ='\t')


# In[ ]:


#Reset Index for upload to database
df_msp_nc = df_msp_nc.reset_index()    


# In[ ]:


#Fill NaN values for upload to database
df_msp_nc['Metro'] = df_msp_nc['Metro'].replace(np.nan,'', regex=True)

column_list = df_msp_nc.columns.values
for i in column_list:
    df_msp_nc.loc[df_msp_nc[i].isnull(),i]=0


# In[ ]:


#Connect to database and create cursor
con = pyodbc.connect('Driver={SQL Server};'
                      'Server=[servername];'
                      'Database=[dbname];'
                      'Trusted_Connection=yes;',
                    autocommit=True)

c = con.cursor()


# In[ ]:


#Drop old backup table
c.execute('drop table STG_ZLLW_County_MedianSalePrice_AllHomes_BACKUP')


# In[ ]:


#Create new backup
c.execute('''sp_rename 'dbo.STG_ZLLW_County_MedianSalePrice_AllHomes','STG_ZLLW_County_MedianSalePrice_AllHomes_BACKUP';''')


# In[ ]:


c.execute('''USE [[dbname]]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_ZLLW_County_MedianSalePrice_AllHomes](
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
                                 r'Server=[servername];'
                                 r'Database=[dbname];'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df_msp_nc.to_sql('STG_ZLLW_County_MedianSalePrice_AllHomes', con=engine, if_exists='replace', index=False)

