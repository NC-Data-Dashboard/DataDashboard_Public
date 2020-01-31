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
df_backup = pd.read_csv('./Updates/STG_ZLLW_County_Zhvi_AllHomes.txt')
df_backup.to_csv('./Backups/STG_ZLLW_County_Zhvi_AllHomes_BACKUP.txt')


# In[ ]:


#Load Land data
df_zhvi = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_Zhvi_AllHomes.csv', 
                      encoding='ISO-8859-1')

#Display table to ensure data loaded correctly
df_zhvi.head()


# In[ ]:


#Filter data to NC
filter1 = df_zhvi['State'] == "NC"
df_zhvi_nc = df_zhvi[filter1]

#Check to ensure filter worked
df_zhvi_nc.head(5)


# In[ ]:


#View data types of dataframe
df_zhvi_nc.dtypes


# In[ ]:


#Change MunicipalCodeFIPS dtype to add leading 0's
df_zhvi_nc.loc[ :, 'MunicipalCodeFIPS'] = df_zhvi_nc['MunicipalCodeFIPS'].astype(str)
df_zhvi_nc.dtypes


# In[ ]:


#Add leading 0's and check to ensure they were added
df_zhvi_nc.loc[ :, 'MunicipalCodeFIPS'] = df_zhvi_nc['MunicipalCodeFIPS'].str.zfill(3)
df_zhvi_nc.head(5)


# In[ ]:


# Set Index to Region Name
df_zhvi_nc.set_index(df_zhvi_nc['RegionName'], inplace = True)
df_zhvi_nc.head(5)


# In[ ]:


# Drop Region Name column
df_zhvi_nc.drop('RegionName', axis = 1, inplace = True)
df_zhvi_nc.head(5)


# In[ ]:


#Save to csv file for export in Excel
df_zhvi_nc.to_csv('./Updates/STG_ZLLW_County_Zhvi_AllHomes.txt', sep = '\t')


# In[ ]:


#Reset Index for upload to database
df_zhvi_nc = df_zhvi_nc.reset_index()    


# In[ ]:


#Fill NaN values for upload to database
df_zhvi_nc['Metro'] = df_zhvi_nc['Metro'].replace(np.nan,'', regex=True)

column_list = df_zhvi_nc.columns.values
for i in column_list:
    df_zhvi_nc.loc[df_zhvi_nc[i].isnull(),i]=0


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
c.execute('drop table STG_ZLLW_County_Zhvi_AllHomes_BACKUP')


# In[ ]:


#Create new backup
c.execute('''sp_rename 'dbo.STG_ZLLW_County_Zhvi_AllHomes','STG_ZLLW_County_Zhvi_AllHomes_BACKUP';''')


# In[ ]:


c.execute('''USE [DataDashboard]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_ZLLW_County_Zhvi_AllHomes](
	[RegionName] [varchar](19) NULL,
	[RegionID] [smallint] NULL,
	[State] [varchar](2) NULL,
	[Metro] [varchar](38) NULL,
	[StateCodeFIPS] [varchar](2) NULL,
	[MunicipalCodeFIPS] [varchar](3) NULL,
	[SizeRank] [smallint] NULL,
	[1996-04] [float] NULL,
	[1996-05] [float] NULL,
	[1996-06] [float] NULL,
	[1996-07] [float] NULL,
	[1996-08] [float] NULL,
	[1996-09] [float] NULL,
	[1996-10] [float] NULL,
	[1996-11] [float] NULL,
	[1996-12] [float] NULL,
	[1997-01] [float] NULL,
	[1997-02] [float] NULL,
	[1997-03] [float] NULL,
	[1997-04] [float] NULL,
	[1997-05] [float] NULL,
	[1997-06] [float] NULL,
	[1997-07] [float] NULL,
	[1997-08] [float] NULL,
	[1997-09] [float] NULL,
	[1997-10] [float] NULL,
	[1997-11] [float] NULL,
	[1997-12] [float] NULL,
	[1998-01] [float] NULL,
	[1998-02] [float] NULL,
	[1998-03] [float] NULL,
	[1998-04] [float] NULL,
	[1998-05] [float] NULL,
	[1998-06] [float] NULL,
	[1998-07] [float] NULL,
	[1998-08] [float] NULL,
	[1998-09] [float] NULL,
	[1998-10] [float] NULL,
	[1998-11] [float] NULL,
	[1998-12] [float] NULL,
	[1999-01] [float] NULL,
	[1999-02] [float] NULL,
	[1999-03] [float] NULL,
	[1999-04] [float] NULL,
	[1999-05] [float] NULL,
	[1999-06] [float] NULL,
	[1999-07] [float] NULL,
	[1999-08] [float] NULL,
	[1999-09] [float] NULL,
	[1999-10] [float] NULL,
	[1999-11] [float] NULL,
	[1999-12] [float] NULL,
	[2000-01] [float] NULL,
	[2000-02] [float] NULL,
	[2000-03] [float] NULL,
	[2000-04] [float] NULL,
	[2000-05] [float] NULL,
	[2000-06] [float] NULL,
	[2000-07] [float] NULL,
	[2000-08] [float] NULL,
	[2000-09] [float] NULL,
	[2000-10] [float] NULL,
	[2000-11] [float] NULL,
	[2000-12] [float] NULL,
	[2001-01] [float] NULL,
	[2001-02] [float] NULL,
	[2001-03] [float] NULL,
	[2001-04] [float] NULL,
	[2001-05] [float] NULL,
	[2001-06] [float] NULL,
	[2001-07] [float] NULL,
	[2001-08] [float] NULL,
	[2001-09] [float] NULL,
	[2001-10] [float] NULL,
	[2001-11] [float] NULL,
	[2001-12] [float] NULL,
	[2002-01] [float] NULL,
	[2002-02] [float] NULL,
	[2002-03] [float] NULL,
	[2002-04] [float] NULL,
	[2002-05] [float] NULL,
	[2002-06] [float] NULL,
	[2002-07] [float] NULL,
	[2002-08] [float] NULL,
	[2002-09] [float] NULL,
	[2002-10] [float] NULL,
	[2002-11] [float] NULL,
	[2002-12] [float] NULL,
	[2003-01] [float] NULL,
	[2003-02] [float] NULL,
	[2003-03] [float] NULL,
	[2003-04] [float] NULL,
	[2003-05] [float] NULL,
	[2003-06] [float] NULL,
	[2003-07] [float] NULL,
	[2003-08] [float] NULL,
	[2003-09] [float] NULL,
	[2003-10] [float] NULL,
	[2003-11] [float] NULL,
	[2003-12] [float] NULL,
	[2004-01] [float] NULL,
	[2004-02] [float] NULL,
	[2004-03] [float] NULL,
	[2004-04] [float] NULL,
	[2004-05] [float] NULL,
	[2004-06] [float] NULL,
	[2004-07] [float] NULL,
	[2004-08] [float] NULL,
	[2004-09] [float] NULL,
	[2004-10] [float] NULL,
	[2004-11] [float] NULL,
	[2004-12] [float] NULL,
	[2005-01] [float] NULL,
	[2005-02] [float] NULL,
	[2005-03] [float] NULL,
	[2005-04] [float] NULL,
	[2005-05] [float] NULL,
	[2005-06] [float] NULL,
	[2005-07] [float] NULL,
	[2005-08] [float] NULL,
	[2005-09] [float] NULL,
	[2005-10] [float] NULL,
	[2005-11] [float] NULL,
	[2005-12] [float] NULL,
	[2006-01] [float] NULL,
	[2006-02] [float] NULL,
	[2006-03] [float] NULL,
	[2006-04] [float] NULL,
	[2006-05] [float] NULL,
	[2006-06] [float] NULL,
	[2006-07] [float] NULL,
	[2006-08] [float] NULL,
	[2006-09] [float] NULL,
	[2006-10] [float] NULL,
	[2006-11] [float] NULL,
	[2006-12] [float] NULL,
	[2007-01] [float] NULL,
	[2007-02] [float] NULL,
	[2007-03] [float] NULL,
	[2007-04] [float] NULL,
	[2007-05] [float] NULL,
	[2007-06] [float] NULL,
	[2007-07] [float] NULL,
	[2007-08] [float] NULL,
	[2007-09] [float] NULL,
	[2007-10] [float] NULL,
	[2007-11] [float] NULL,
	[2007-12] [float] NULL,
	[2008-01] [float] NULL,
	[2008-02] [float] NULL,
	[2008-03] [float] NULL,
	[2008-04] [float] NULL,
	[2008-05] [float] NULL,
	[2008-06] [float] NULL,
	[2008-07] [float] NULL,
	[2008-08] [float] NULL,
	[2008-09] [float] NULL,
	[2008-10] [float] NULL,
	[2008-11] [float] NULL,
	[2008-12] [float] NULL,
	[2009-01] [float] NULL,
	[2009-02] [float] NULL,
	[2009-03] [float] NULL,
	[2009-04] [float] NULL,
	[2009-05] [float] NULL,
	[2009-06] [float] NULL,
	[2009-07] [float] NULL,
	[2009-08] [float] NULL,
	[2009-09] [float] NULL,
	[2009-10] [float] NULL,
	[2009-11] [float] NULL,
	[2009-12] [float] NULL,
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
	[2019-12] [float] NULL
) ON [PRIMARY]''')


# In[ ]:


params = urllib.parse.quote_plus(r'Driver={SQL Server};' 
                                 r'Server=TITANIUM-BOOK;'
                                 r'Database=DataDashboard;'
                                 r'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#df: pandas.dataframe; mTableName:table name in MS SQL
#warning: discard old table if exists
df_zhvi_nc.to_sql('STG_ZLLW_County_Zhvi_AllHomes', con=engine, if_exists='replace', index=False)

